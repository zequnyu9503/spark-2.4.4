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

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

protected [timewindow] sealed class TimeWindowController[T, V](
    val sc: SparkContext,
    val size: Long,
    val step: Long,
    val func: (T, T) => RDD[(T, V)]) extends Logging{

  private val entries =
    new util.LinkedHashMap[Integer, RDD[(T, V)]](32, 0.75f, true)
  private val winId = new AtomicInteger(0)
  private var partition: Integer = 0
  private var minMemRequire: Long = 0
  private val minOne = Math.min(size, step)
  private val maxOne = Math.max(size, step)

  var scope = TimeScope()
  var keepInMem: Integer = 1
  var keepInMemCapacity = Long.MaxValue
  var storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
  var partitionLimitations: Integer = partition

  def isEmpty: Boolean = {
    var isEpt: Boolean = true
    if (scope.isDefault) {
      // 保存当前时间参数.
      TimeWindowController.save()
      update()
      nextRDD() match {
        case Some(coarseRDD) =>
          isEpt = coarseRDD.isEmpty()
        case _ =>
      }
      TimeWindowController.reset()
    } else {
      isEpt = !scope.isLegal(TimeWindowController.winStart)
    }
    isEpt
  }

  def next(): RDD[(T, V)] = {
    if (entries.size().equals(1)) minMemRequire = memUsedForCache()
    val isMemSufficient = checkMemCapacity()
    update(isMemSufficient)
    clean(keepInMem)
    nextRDD(isMemSufficient) match {
      case Some(coarseRDD) =>
        entries.put(winId.getAndIncrement(), coarseRDD)
        coarseRDD
      case _ => null
    }
  }

  private def nextRDD(
      cached: Boolean = false): Option[RDD[(T, V)]] = {
    try {
      // suffixRDD 只能从非缓存空间获取.
      val suffixRDD = func(TimeWindowController.startTime.asInstanceOf[T],
                           TimeWindowController.endTime.asInstanceOf[T])
      latestCachedRDD() match {
        case Some(rdd) =>
          // 如果缓存RDD存在则意味着窗口之间存在重叠.
          var prefixRDD = rdd.filter(_._1.asInstanceOf[Long] >= TimeWindowController.winStart)
          if (partitionsReserved() > 0) prefixRDD = prefixRDD.coalesce(partitionsReserved())
          var coarseRDD = prefixRDD.union(suffixRDD)
          if (cached) coarseRDD = coarseRDD.
              persist(storageLevel)
                .setName(s"TimeWindowRDD[${winId.get()}]")
          Option(coarseRDD)
        case None =>
          if (partition.equals(0)) partition = suffixRDD.getNumPartitions
          var coarseRDD = suffixRDD
          if (cached) coarseRDD = coarseRDD.
            persist(storageLevel).
            setName(s"TimeWindowRDD[${winId.get()}]")
          Option(coarseRDD)
      }
    } catch {
      case e: Exception =>
        // scalastyle:off println
        System.err.println(e.printStackTrace())
        // scalastyle:on println
        None
    }
  }

  private def update(cached: Boolean = false): Unit = {
    if (TimeWindowController.initialized) {
      if (cached) {
        TimeWindowController.startTime = TimeWindowController.winStart + maxOne
        TimeWindowController.endTime = TimeWindowController.startTime + minOne
      } else {
        TimeWindowController.startTime = TimeWindowController.winStart + step
        TimeWindowController.endTime = TimeWindowController.startTime + size
      }
      TimeWindowController.winStart += step
    } else {
      TimeWindowController.winStart = scope.start
      TimeWindowController.startTime = TimeWindowController.winStart
      TimeWindowController.endTime = TimeWindowController.winStart + size
      TimeWindowController.initialized = !TimeWindowController.initialized
    }
  }

  private def disable(id: Integer): TimeWindowController[T, V] = {
    if (entries.containsKey(id)) {
      entries.get(id).unpersist(true)
    }
    this
  }

  private def remove(id: Integer): TimeWindowController[T, V] = {
    if (entries.containsKey(id)) {
      entries.remove(id)
    }
    this
  }

  private def clean(keepInMem: Integer): Unit = {
    if (entries.size() > keepInMem) {
      val delList = new util.ArrayList[Integer]
      val delLimit = latestWinId() - (entries.size() - keepInMem)
      val itr = entries.entrySet().iterator()
      while (itr.hasNext) {
        val rddId = itr.next().getKey
        if (rddId <= delLimit) delList.add(rddId)
      }
      val itr_ = delList.iterator()
      while (itr_.hasNext) {
        val rddId = itr_.next()
        this.disable(rddId).remove(rddId)
      }
    }
  }

  private def rddSizeInMem(winId: Integer): Long = {
    sc.getRDDStorageInfo.find(_.id.equals(rddId(winId))) match {
      case Some(info) => info.memSize
      case _ => -1
    }
  }

  private def checkMemCapacity(): Boolean = if (!minMemRequire.equals(0)) {
    if (keepInMemCapacity < minMemRequire) {
      // scalastyle:off println
      System.err.println("Cannot persist TimeWindowRDD in memory")
      // scalastyle:on println
      false
    } else {
      // 当前缓存RDD所占用的内存量.
      val memUsed = memUsedForCache()
      var freeMemReserved = keepInMemCapacity - memUsed
      while (freeMemReserved < minMemRequire && entries.size() > 1) {
          clean(entries.size() - 1)
        freeMemReserved = keepInMemCapacity - memUsed
      }
      if (freeMemReserved < minMemRequire) false else true
    }
  } else true

  private def memUsedForCache(): Long = {
    var memSize = 0L
    var id = latestWinId()
    while (entries.containsKey(id)) {
      memSize += rddSizeInMem(id)
      id -= 1
    }
    memSize
  }

  private def rddId(winId: Integer): Integer = latestCachedRDD(winId) match {
    case Some(rdd) => rdd.id
    case _ => -1
  }

  private def latestCachedRDD(
      n: Integer = latestWinId()): Option[RDD[(T, V)]] = {
    if (entries.containsKey(n)) Option(entries.get(n)) else None
  }

  private def latestWinId(): Integer = {
    val nextId = winId.get()
    if (nextId > 0) nextId - 1 else nextId
  }

  private def partitionsReserved(): Integer = {
    if (partitionLimitations > partition) {
      partitionLimitations - partition
    } else {
      partition
    }
  }
}

protected [timewindow] object TimeWindowController {
  var initialized: Boolean = false
  var winStart: Long = 0L
  var startTime: Long = 0L
  var endTime: Long = 0L

  var record: (Long, Long, Long, Boolean) =
    (winStart, startTime, endTime, initialized)

  def save(): Unit =
    record = (winStart, startTime, endTime, initialized)

  def reset(): Unit = {
    winStart = record._1
    startTime = record._2
    endTime = record._3
    initialized = record._4
  }
}
