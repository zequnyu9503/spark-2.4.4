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
  private val expansion_ = new mutable.HashMap[Int, Double]()

  // Historical time window data size.
  private val winSize = new mutable.HashMap[Int, Long]()

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
      if (winSize.contains(id)) {
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

  private def expansion(winId: Int): Double = {
    if (expansion_.contains(winId)) {
      expansion_(winId)
    } else {
      // Average expansion provided.
      expansion_.values.sum / expansion_.size
    }
  }

  private def prefetch_duration(plan: PrefetchPlan): Long = {
    val size: Long = randomWinSize(plan.winId).getOrElse(Long.MaxValue)
    val partitionSize: Long = size / plan.prefetch.partitions.length
    val batches = plan.maxLocality.map {
      case TaskLocality.NODE_LOCAL => load_local * partitionSize
      case TaskLocality.ANY => load_remote * partitionSize
      case _ => 0L
    }
    val duration = batches.sum.toLong
    logger.info(s"Prefetch duration >> window size: [$size], " +
      s"load_local: [$load_local], duration: [$duration].")
    duration
  }

  private def main_duration(plan: PrefetchPlan): Long = {
    var waiting: Double = 0d
    for (id <- winId until plan.winId) {
      randomWinSize(id) match {
        case Some(size) =>
          if (finished.contains(id)) {
            waiting += size * calc
          } else {
            waiting += size * (calc + load_local)
          }
        case None => waiting += 0
      }
    }
    val used = System.currentTimeMillis() - startLine.maxBy(_._1)._2
    val main = (waiting - used).toLong
    logger.info(s"Main duration >> Waiting: [$waiting], Used: [$used], Main: [$main].")
    main
  }

  private def prefetch_requirement(plan: PrefetchPlan): Long = {
    val requirement = randomWinSize(plan.winId) match {
      case Some(size) => (size * expansion(plan.winId)).toLong
      case None => Long.MaxValue
    }
    logger.info(s"Prefetch requirement >> Size: [$requirement].")
    requirement
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
    val availability = currentFreeStorage - enlarged
    logger.info(s"Cluster availability >> currentFreeStorage: [$currentFreeStorage], " +
      s"Enlarged: [$enlarged].")
    availability
  }

  def canPrefetch(plan: PrefetchPlan): Boolean = {
    if (plan.winId > min) {
      val prefetch = prefetch_duration(plan)
      val main = main_duration(plan)
      if (prefetch < main) {
        val requirement = prefetch_requirement(plan)
        val availability = cluster_availability(plan)
        if (requirement < availability) {
          true
        } else false
      } else false
    } else false
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
          finished(id) = plan.rdd
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
  }

  def updateExpansion(winId: Int, factor: Double): Unit = {
    if (!expansion_.contains(winId)) expansion_(winId) = factor
  }
}
