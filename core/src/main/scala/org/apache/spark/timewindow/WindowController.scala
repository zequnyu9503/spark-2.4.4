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

import org.slf4j.LoggerFactory

import org.apache.spark.SparkContext
import org.apache.spark.prefetch.cluster.PrefetchBackend
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class WindowController[T, V, X] (
                               val sc: SparkContext,
                               val size: Long,
                               val step: Long,
                               val func: (T, T) => RDD[(T, V)]) {

  private val logger = LoggerFactory.getLogger("tw")

  // Time window ID.
  val winId = new AtomicInteger(0)

  def id: Int = winId.get()

  private val windows = new mutable.HashMap[Int, RDD[(T, V)]]()

  private val localResults = new mutable.HashMap[Int, RDD[X]]()

  private val timeScope = new TimeScope()

  private var maxPartitions = 120
  private var storageLevel = StorageLevel.MEMORY_ONLY

  // ONLY FOR EXPERIMENT.
  private var daySize: Seq[Long] = _
  private var expanScope: Seq[Double] = _

  private var backend_ : PrefetchBackend = _

  private def backend: Option[PrefetchBackend] = Option(backend_)

  private def timeline(id: Int): (Long, Long) =
    (timeScope.start + id * step, timeScope.start + id * step + size - 1)

  private def isPrefetchd(id: Int): Boolean = {
    backend match {
      case Some(bk) => bk.finished.contains(id)
      case _ => false
    }
  }

  private def prefetched[T, V](id: Int): Option[RDD[(T, V)]] = {
    if (isPrefetchd(id)) {
      Option(backend.get.finished(id).asInstanceOf[RDD[(T, V)]])
    } else None
  }

  private def updateBackend(): Unit = {
    backend match {
      case Some(bk) =>
        bk.updateWinId(winId.get())
        bk.updateStartLine(winId.get(), System.currentTimeMillis())

        for (wId <- daySize.indices) {
          bk.updateWinSize(wId, daySize(wId))
        }

        for (winId <- expanScope.indices) {
          bk.updateExpansion(winId, expanScope(winId))
        }
//        val prevWinId = if (winId.get() > 0) winId.get() - 1 else 0
//        if (windows.contains(prevWinId)) {
//          // BAD OPERATIONS.
//
//        }
      case _ =>
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
          logger.info("Windows overlap and we need to filter previous tw rdd.")
          rdd.filter(_._1.asInstanceOf[Long] >= (line._2 - step)).
            filter(_._1.asInstanceOf[Long] < line._2)
        case None =>
          logger.info("Windows overlap and we need to create suffix tw rdd.")
          func((line._2 - step).asInstanceOf[T], line._2.asInstanceOf[T])
      }
      val prefix = isCached(id - 1) match {
        case Some(rdd) =>
          logger.info("Windows overlap and we need to filter previous tw rdd")
          rdd.filter(_._1.asInstanceOf[Long] >= line._1).
          filter(_._1.asInstanceOf[Long] < line._2 - step)
        case None =>
          logger.info("Windows overlap and we need to create previous tw rdd")
          func(line._1.asInstanceOf[T], (line._2 - step).asInstanceOf[T])
      }
      suffix.union(prefix).persist(storageLevel)
    } else {
      prefetched[T, V](id) match {
        case Some(rdd) =>
          logger.info("No overlaps. Tw rdd exists in memory already.")
          rdd
        case None =>
          logger.info("We need to create tw rdd manually.")
          func(line._1.asInstanceOf[T], line._2.asInstanceOf[T])
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
      logger.info("Previous windows exist and may be cleaned up.")
      underClear.foreach(_._2.unpersist(false))
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

  def startPrefetchService(): Unit = {
    val winFetcher = new WinFetcher[T, V](sc, this, sc.prefetchScheduler)
    backend match {
      case Some(bk) => backend_ = winFetcher.backend
      case _ => logger.warn("Prefetch backend already exists.")
    }
    val thr = new Thread(winFetcher)
    thr.setName("WinFetcher")
    thr.start()
    logger.info("WinFetcher service begins running.")
  }

  def setDaySize(size: Seq[Long]): Unit = {
    daySize = size
  }

  def setExpansion(exp: Seq[Double]): Unit = {
    expanScope = exp
  }

  def addLocalResult(rdd: RDD[X]): Unit = {
    localResults(winId.get()) = rdd
    backend match {
      case Some(bk) =>
        val size = sc.rddCacheInMemory(rdd)
        bk.updateLocalResults(winId.get(), rdd, size)
      case _ =>
    }
  }

  def localAsRDD(): RDD[X] = {
    localResults.values.reduce((a, b) => a.union(b)).coalesce(maxPartitions)
  }

  def next: RDD[(T, V)] = {
    updateBackend()
    release()
    // Create next timewindow rdd.
    val rdd = randomWindow(winId.get())
    windows(winId.getAndIncrement()) = rdd
    rdd
  }

  def hasNext: Boolean = timeScope.isLegal(timeline(winId.get())._1)
}
