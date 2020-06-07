/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.prefetch.cluster

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.prefetch.{DataSizeForecast, PrefetchReporter}
import org.apache.spark.prefetch.scheduler.PrefetchScheduler
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocality

class PrefetchBackend(val sc: SparkContext, val scheduler: PrefetchScheduler) {

  private val logger = LoggerFactory.getLogger("prefetch")

  // Expansion factor for data from disk to memory.
  private var expansion: Double = sc.conf.getDouble("expansion.hdfs", 2d)

  // Historical time window data size.
  private [prefetch] val winSize = new mutable.HashMap[Int, Long]()

  // Historical local results size.
  private val localSize = new mutable.HashMap[Int, Long]()

  // CPU cores for prefetching on each executors.
  private var cores: Int = sc.conf.getInt("cores.prefetch.executors", 4)

  // Velocity of computation.
  // Updated.
  private var calc: Double = sc.conf.getDouble("calc.prefetch", 0)

  // Loading velocity for loading local data.
  // Updated.
  private var load_local: Double = sc.conf.getDouble("load.local.prefetch", 0d)

  // Loading velocity for loading remote data.
  private var load_remote: Double =
    sc.conf.getDouble("load.remote.prefetch", 0d)

  // Variation factor of local result in culster.
  // Updated.
  private var variation: Double = sc.conf.getDouble("variation.prefetch", 0d)

  // Minimize windows accepted for prefetching.
  private var min: Int = sc.conf.getInt("min.prefetch", 3)

  // Forecast tool.
  private val forecast = new DataSizeForecast()

  // Current time window id.
  // Updated.
  @volatile
  private var winId: Int = 0

  // Data size of local results for time windows.
  // Updated.
  private val localResults = new mutable.HashMap[Int, RDD[_]]()

  // Timeline of startup of time window.
  // Updated.
  private val startLine = new mutable.HashMap[Int, Long]()

  // Prefetch in progress or not yet started.
  val pending = new mutable.HashMap[Int, RDD[_]]()

  // Prefetch completed or failed.
  val finished = new mutable.HashMap[Int, RDD[_]]()

  // Forecast future window data size.
  private [prefetch] def randomWinSize(id: Int): Option[Long] = {
    if (winSize.size >= min) {
      if (id < winSize.size) {
        Option(winSize(id))
      } else {
        val history: Array[java.lang.Long] =
          winSize.values.toArray.map(java.lang.Long.valueOf)
        val nextSer = forecast.forecastNextN(history, id - winSize.keySet.max)
        Option(nextSer.get(nextSer.size() - 1).toLong)
      }
    } else {
      None
    }
  }

  private def prefetch_duration(plan: PrefetchPlan): Long = {
    val size: Long = randomWinSize(plan.winId).getOrElse(Long.MaxValue)
    val partitionSize: Long = size / plan.partitions.toLong
    logger.debug(s"Partition size is $partitionSize. ${plan.maxLocality.length} " +
      s"batches needed. load_local is ${load_local.toLong}." +
      s"load_remote is ${load_remote.toLong}")
    val batches = plan.maxLocality.map {
      case TaskLocality.NODE_LOCAL => load_local * partitionSize
      case TaskLocality.ANY => load_remote * partitionSize
      case _ => 0L
    }
    batches.sum.toLong
  }

  private def main_duration(plan: PrefetchPlan): Long = {
    var waiting: Double = 0d
    for (id <- winId until plan.winId) {
      randomWinSize(id) match {
        case Some(size) =>
          if (finished.contains(id)) {
            logger.debug(s"Win $id was cached in memory, calc only.")
            waiting += size * calc
          } else {
            logger.debug(s"Win $id was NOT cached in memory, calc and load.")
            waiting += size * (calc + load_local)
          }
        case None => waiting += 0
      }
    }
    val used = System.currentTimeMillis() - startLine.maxBy(_._1)._2
    logger.debug(s"Current window has consume $used millisecond")
    (waiting - used).toLong
  }

  private def prefetch_requirement(plan: PrefetchPlan): Long = {
    randomWinSize(plan.winId) match {
      case Some(size) => (size * expansion).toLong
      case None => Long.MaxValue
    }
  }

  private def cluster_availability(plan: PrefetchPlan): Long = {
    val currentFreeStorage = scheduler.freeStorageMemory().values.sum
    var enlarged: Long = 0L
    for (id <- winId until plan.winId) {
      randomWinSize(id) match {
        case Some(size) => enlarged += (size * variation).toLong
        case None => enlarged += 0L
      }
    }
    logger.debug(s"Culster free: $currentFreeStorage. Expansion of local result: $enlarged")
    currentFreeStorage - enlarged
  }

  def canPrefetch(plan: PrefetchPlan): Boolean = {
    if (plan.winId > min) {
      val prefetch = prefetch_duration(plan)
      logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
        s"Prefetch duration: $prefetch ms.")
      val main = main_duration(plan)
      logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
        s"Waiting duration: $main ms.")

      if (prefetch < main) {
        val requirement = prefetch_requirement(plan)
        logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
          s"Prefetch require $requirement bytes of memory space.")
        val availability = cluster_availability(plan)
        logger.debug(
          s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
            s"The cluster can only provide $availability bytes of memory.")

        if (requirement < availability) {
          logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
            "Trigger time and space condition.")
          true
        } else {
          logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
            "Prefetch require more space than cluster.")
          false
        }
      } else {
        logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
          s"Prefetch duration is more than main duration. ")
        false
      }
    } else {
      logger.debug(s"Attempt to check plan [${plan.winId}] while winId [$winId];" +
        s"Failed: ${plan.winId} is less than $min.")
      false
    }
  }

  def updateVelocity(plan: PrefetchPlan, reporters: Seq[PrefetchReporter]): Unit = {
    val velocity_local = new ArrayBuffer[Double]()
    val velocity_remote = new ArrayBuffer[Double]()
    plan.schedule.flatten.foreach(desc => {
      val duration = reporters.find(_.taskId.equals(desc.taskId)) match {
        case Some(reporter) => reporter.duration
        case _ => 0L
      }
      val size = scheduler.blockSize(plan.prefetch, desc.taskId.toInt, desc.executorId)
      if (size > 0) {
        // Make sure the data are loaded into memory.
        desc.locality match {
          case TaskLocality.NODE_LOCAL =>
            velocity_local += (duration.toDouble / size.toDouble)
          case TaskLocality.ANY =>
            velocity_remote += (duration.toDouble / size.toDouble)
        }
      }
    })
    if (velocity_local.nonEmpty) load_local = velocity_local.max
    if (velocity_remote.nonEmpty) load_remote = velocity_remote.max
  }

  def doPrefetch(plan: PrefetchPlan): Unit = {
    val id = plan.winId
    if (!pending.contains(id) && !finished.contains(id)) {
      pending(id) = plan.prefetch

      logger.info(s"Start prefetching time window [$id].")
      scheduler.prefetch(plan.prefetch) match {
        case Some(reporters) =>
          logger.info(s"Pefetch ${plan.prefetch.id} successfully. Then update it.")
          updateVelocity(plan, reporters)
          finished(id) = plan.prefetch
        case _ =>
      }
      pending.remove(id)
    }
  }

  def updateWinId(id: Int): Unit = synchronized {
    winId = id
    logger.info(s"Update current winId [$id].")
  }

  def updateStartLine(id: Int, start: Long): Unit = synchronized {
    if (!startLine.contains(id)) {
      startLine(id) = start
      logger.info(s"Update current window's startline [$start].")
    } else {
      logger.info("Update failed: winId already exists.")
    }
  }

  def updateLocalResults(id: Int, rdd: RDD[_], size: Long): Unit = synchronized {
    if (!localResults.contains(id)) {
      localResults(id) = rdd
      localSize(id) = size
      logger.info(s"Update local results [${rdd.id}] size of [$size] bytes.")
    } else {
      logger.info("Update failed: winId already exists.")
    }
  }

  def updateWinSize(winId: Int, size: Long): Unit = {
    if (!winSize.contains(winId)) winSize(winId) = size
    logger.info(s"Update window [$winId] size of [$size] bytes on disk.")
  }
}
