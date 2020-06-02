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

  private val localResults = new mutable.HashMap[Int, RDD[X]]()

  private val timeScope = new TimeScope()

  private var maxPartitions = 10
  private var storageLevel = StorageLevel.MEMORY_ONLY

  private var backend: PrefetchBackend = _

  private def timeline(id: Int): (Long, Long) =
    (timeScope.start + id * step, timeScope.start + id * step + size - 1)

  private def isPrefetchd(id: Int): Boolean = {
    !backend.eq(null) && backend.finished.contains(id)
  }

  private def prefetched[T, V](id: Int): Option[RDD[(T, V)]] = {
    if (isPrefetchd(id)) {
      Option(backend.finished(id).asInstanceOf[RDD[(T, V)]])
    } else {
      None
    }
  }

  private def updateBackend(): Unit = {
    if (!backend.eq(null)) {
      logInfo(s"Update prefetch backend with its winId" +
        s" [${winId.get()}] and start line.")
      backend.updateWinId(winId.get())
      backend.updateStartLine(winId.get(), System.currentTimeMillis())
    } else {
      logError("PrefetchBackend is not working currently.")
    }
  }

  private def isCached(id: Int): Option[RDD[(T, V)]] = {
    if (windows.contains(id)) {
      if (sc.persistentRdds.contains(windows(id).id)) {
        Option(windows(id))
      }
    }
    None
  }

  def randomWindow(id: Int): RDD[(T, V)] = {
    val line = timeline(id)
    if (step < size && id - 1 >=0) {
      // This means that windows overlaps.
      val suffix = isCached(id) match {
        case Some(rdd) =>
          logInfo("Windows overlap and we need to filter previous tw rdd.")
          rdd.filter(_._1.asInstanceOf[Long] >= (line._2 - step)).
            filter(_._1.asInstanceOf[Long] < line._2)
        case None =>
          logInfo("Windows overlap and we need to create suffix tw rdd.")
          func((line._2 - step).asInstanceOf[T], line._2.asInstanceOf[T])
      }
      val prefix = isCached(id - 1) match {
        case Some(rdd) =>
          logInfo("Windows overlap and we need to filter previous tw rdd")
          rdd.filter(_._1.asInstanceOf[Long] >= line._1).
          filter(_._1.asInstanceOf[Long] < line._2 - step)
        case None =>
          logInfo("Windows overlap and we need to create previous tw rdd")
          func(line._1.asInstanceOf[T], (line._2 - step).asInstanceOf[T])
      }
      suffix.union(prefix).persist(storageLevel)
    } else {
      prefetched[T, V](id) match {
        case Some(rdd) =>
          logInfo("No overlaps. Tw rdd exists in memory already.")
          rdd
        case None =>
          logInfo("We need to create tw rdd manually.")
          func(line._1.asInstanceOf[T], line._2.asInstanceOf[T]).persist(storageLevel)
      }
    }
  }

  private def release(): Unit = {
    val underClear = if (step < size && winId.get() - 1 > 0) {
      windows.filter(_._1 < winId.get() - 1)
    } else {
      windows.filter(_._1 < winId.get())
    }
    if (underClear.nonEmpty) {
      logInfo("Windows exist and may be cleaned up..")
      underClear.filter(meta => sc.persistentRdds.contains(meta._2.id)).
        foreach(meta => {
          backend.updateWinSize(meta._1, backend.scheduler.sizeInMem(meta._2))
          meta._2.unpersist(false)
        })
    }
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
    logInfo("Initialize prefetch backend.")
    backend = pb
  }

  def addLocalResult(rdd: RDD[X]): Unit = {
    localResults(winId.get()) = rdd
    if (!backend.eq(null)) {
      backend.updateLocalResults(winId.get(), rdd)
    }
  }

  def localAsRDD(): RDD[X] = {
    localResults.values.reduce((a, b) => a.union(b))
  }

  def next: RDD[(T, V)] = {
    release()
    updateBackend()
    // Create next timewindow rdd.
    val rdd = randomWindow(winId.get())
    windows(winId.getAndIncrement()) = rdd
    rdd
  }

  def hasNext: Boolean = timeScope.isLegal(timeline(winId.get())._1)
}
