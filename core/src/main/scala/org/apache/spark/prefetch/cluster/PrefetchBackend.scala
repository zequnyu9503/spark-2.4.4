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

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.prefetch.DataSizeForecast
import org.apache.spark.prefetch.scheduler.PrefetchScheduler
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocality

class PrefetchBackend(sc: SparkContext, scheduler: PrefetchScheduler)
    extends Logging {

  // Expansion factor for data from disk to memory.
  private var expansion: Double = sc.conf.getDouble("expansion.hdfs", 2d)

  // Historical time window data size.
  private val winSize = new mutable.HashMap[Int, Long]()

  // CPU cores for prefetching on each executors.
  private var cores: Int = sc.conf.getInt("cores.prefetch.executors", 4)

  // Velocity of computation.
  private var calc: Double = sc.conf.getLong("calc.prefetch", 0)

  // Loading velocity for loading local data.
  private var load_local: Double = sc.conf.getLong("load.local.prefetch", 0L)

  // Loading velocity for loading remote data.
  private var load_remote: Double = sc.conf.getLong("load.remote.prefetch", 0L)

  // Variation factor of local result in culster.
  private var variation: Double = sc.conf.getDouble("variation.prefetch", 0d)

  // Minimize windows accepted for prefetching.
  private var min: Int = sc.conf.getInt("min.prefetch", 3)

  // Forecast tool.
  private val forecast = new DataSizeForecast()

  // Current time window id.
  @volatile
  private var winId: Int = 0

  // Data size of local results for time windows.
  private val localResults = new mutable.HashMap[Int, RDD[_]]()

  // Timeline of startup of time window.
  private val startLine = new mutable.HashMap[Int, Long]()

  // Prefetch in progress or not yet started.
  val pending = new mutable.HashMap[Int, RDD[_]]()

  // Prefetch completed or failed.
  val finished = new mutable.HashMap[Int, RDD[_]]()

  // Forecast future window data size.
  private def randomWinSize(id: Int): Option[Long] = {
    if (winSize.size >= min) {
      if (id < winSize.size) {
        Option(winSize(id))
      } else {
        val history: Array[java.lang.Long] =
          winSize.values.toArray.map(java.lang.Long.valueOf)
        val nextSeri = forecast.forecastNextN(history, id - winSize.keySet.max)
        Option(nextSeri.get(nextSeri.size() - 1).toLong)
      }
    } else {
      None
    }
  }

  private def prefetch_duration(plan: PrefetchPlan): Long = {
    val size: Long = randomWinSize(plan.winId).getOrElse(winSize.keySet.max)
    val partitionSize: Long = size / plan.partitions.toLong
    val batches = plan.maxLocality.map {
        case TaskLocality.NODE_LOCAL => load_local * partitionSize
        case TaskLocality.ANY => load_remote * partitionSize
        case _ => 0L
      }
    batches.sum.toLong
  }

  private def main_duration(plan: PrefetchPlan): Long = {
    var waiting: Long = 0
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
    waiting - used
  }

  private def prefetch_requirement(plan: PrefetchPlan): Long = {
    randomWinSize(plan.winId) match {
      case Some(size) => (size.toDouble * expansion).toLong
      case None => 0L
    }
  }

  private def cluster_availability(plan: PrefetchPlan): Long = {
    val currentFreeStorage = scheduler.freeStorageMemory().values.sum
    val local = localResults.values.map(rdd => scheduler.sizeInMem(rdd)).sum
    var enlarged: Long = 0L
    for (id <- winId until plan.winId) {
      randomWinSize(id) match {
        case Some(size) => enlarged += (size * variation).toLong
        case None => enlarged += 0L
      }
    }
    currentFreeStorage - (local + enlarged)
  }

  def canPrefetch(plan: PrefetchPlan): Boolean = {
    if (plan.winId > min) {
      val prefetch = prefetch_duration(plan)
      val main = main_duration(plan)

      logInfo(s"prefetch: $prefetch main: $main")

      if (prefetch < main) {
        val requirement = prefetch_requirement(plan)
        val availability = cluster_availability(plan)

        logInfo(s"requirement: $requirement availability: $availability")

        if (requirement < availability) true else false
      } else false
    } else false
  }

  def doPrefetch(plan: PrefetchPlan): Unit = {
    val id = plan.winId
    if (!pending.contains(id) && !finished.contains(id)) {
      pending(id) = plan.rdd

      scheduler.prefetch(plan.rdd)

      finished(id) = plan.rdd
      pending.remove(id)
    }
  }

  def updateWinId(id: Int): Unit = synchronized {
    winId = id
    logInfo(s"Update winId $id")
  }

  def updateStartLine(id: Int, start: Long): Unit = synchronized {
    if (!startLine.contains(id)) {
      startLine(id) = start
      logInfo(s"Update startline: add $id")
    } else {
      logError("Update failed: winId already exists.")
    }
  }

  def updateLocalResults(id: Int, rdd: RDD[_]): Unit = synchronized {
    if (!localResults.contains(id)) {
      localResults(id) = rdd
      logInfo(s"Update local results: add $id")
    } else {
      logError("Update failed: winId already exists.")
    }
  }
}
