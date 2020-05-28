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
package org.apache.spark.timewindow

import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.prefetch.cluster.PrefetchBackend
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class WindowController[T, V, X] (
                               val sc: SparkContext,
                               val size: Long,
                               val step: Long,
                               val func: (T, T) => RDD[(T, V)]) extends Logging {

  // Time window ID.
  val winId = new AtomicInteger(0)

  private val windows = new mutable.HashMap[Int, RDD[(T, V)]]()

  private val prefetched = new mutable.HashMap[Int, RDD[(T, V)]]()

  private val localResults = new mutable.HashMap[Int, RDD[X]]()

  private val timeScope = new TimeScope()

  private var maxPartitions = 10
  private var storageLevel = StorageLevel.MEMORY_ONLY

  private var backend: PrefetchBackend = _

  private def timeline(id: Int): (Long, Long) =
    (timeScope.start + id * step, timeScope.start + id * step + size - 1)

  private def updateBackend(): Unit = {
    if (!backend.eq(null)) {
      backend.updateWinId(winId.get())
      backend.updateStartLine(winId.get(), System.currentTimeMillis())
    }
  }

  private def isCached(id: Int): Option[RDD[(T, V)]] = {
    if (windows.contains(id)) {
      if (sc.persistentRdds.contains(windows(id).id)) {
        return Option(windows(id))
      }
    }
    if (prefetched.contains(id)) {
      return Option(prefetched(id))
    }
    None
  }

  def randomWindow(id: Int): RDD[(T, V)] = {
    val line = timeline(id)
    if (step < size && id - 1 >=0) {
      // This means that windows overlaps.
      val suffix = isCached(id) match {
        case Some(rdd) => return rdd
        case None => func((line._2 - step).asInstanceOf[T],
          line._2.asInstanceOf[T])
      }
      val prefix = isCached(id - 1) match {
        case Some(rdd) => rdd.
          filter(_._1.asInstanceOf[Long] >= line._1).
          filter(_._1.asInstanceOf[Long] < line._2 - step)
        case None => func(line._1.asInstanceOf[T],
          (line._2 - step).asInstanceOf[T])
      }
      suffix.union(prefix).persist(storageLevel)
    } else {
      if (prefetched.contains(id)) {
        prefetched(id)
      } else {
        func(line._1.asInstanceOf[T],
          line._2.asInstanceOf[T])
      }
    }
  }

  private def release(): Unit = {
    val rdds = if (step < size && winId.get() - 1 > 0) {
      windows.filter(_._1 < winId.get() - 1).values
    } else {
      windows.filter(_._1 < winId.get()).values
    }
    rdds.filter(rdd => sc.persistentRdds.contains(rdd.id)).
      foreach(_.unpersist(false))
  }

  def setTimeScope(start: Long, end: Long): Unit = {
    timeScope.start = start
    timeScope.end = end
  }

  def setStorageLevel(level: StorageLevel): Unit = {
    storageLevel = level
  }

  def setMaxPartitions(partitions: Int): Unit = {
    maxPartitions = partitions
  }

  def setBackend(pb: PrefetchBackend): Unit = {
    backend = pb
  }

  def addLocalResult(rdd: RDD[X]): Unit = {
    localResults(winId.get()) = rdd
    if (!backend.eq(null)) {
      backend.updateLocalResults(winId.get(), rdd)
    }
  }

  def localAsRDD(): RDD[X] = {
    var result = sc.emptyRDD[X]
    localResults.values.foreach(rdd => result = result.union(rdd))
    result
  }

  def next: RDD[(T, V)] = {
    // Release previous cached timewindow rdd.
    release()
    updateBackend()
    // Create next timewindow rdd.
    val rdd = randomWindow(winId.get())
    windows(winId.getAndIncrement()) = rdd
    rdd
  }

  def hasNext: Boolean = timeScope.isLegal(timeline(winId.get())._1)
}
