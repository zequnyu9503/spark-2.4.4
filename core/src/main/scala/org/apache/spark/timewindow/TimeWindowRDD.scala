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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

sealed class TimeWindowRDD[T, V](sc: SparkContext, winSize: T,
                                 winStep: T, func: (T, T) => RDD[(T, V)]) {

  private val controller = new TimeWindowController(sc,
    winSize.asInstanceOf[Long], winStep.asInstanceOf[Long], func)

  private var _iterator: TimeWindowRDDIterator[T, V] = _

  def iterator(): TimeWindowRDDIterator[T, V] = {
    if (_iterator.eq(null)) _iterator = new TimeWindowRDDIterator[T, V](this)
    _iterator
  }

  def setScope(start: T, end: T): TimeWindowRDD[T, V] = {
    controller.scope =
      TimeScope(start.asInstanceOf[Long], end.asInstanceOf[Long])
    this
  }

  def setKeepInMem(n: Integer): TimeWindowRDD[T, V] = {
    if (n > 0) controller.keepInMem = n
    this
  }

  def setStorageLevel(level: StorageLevel): TimeWindowRDD[T, V] = {
    if (level.useMemory && !level.useDisk) {
      controller.storageLevel = level
    }
    this
  }

  def setKeepInMemCapacity(size: Long): TimeWindowRDD[T, V] = {
    if (size > 0) controller.keepInMemCapacity = size
    this
  }

  def setPartitionsLimitations(n: Integer): TimeWindowRDD[T, V] = {
    if (n > 0) controller.partitionLimitations = n
    this
  }

  def allowPrefetch(bool: Boolean): Unit = {

  }

  protected[timewindow] def nextWinRDD(): RDD[(T, V)] = {
    controller.next()
  }

  protected[timewindow] def isNextEmpty: Boolean = {
    controller.isEmpty
  }
}
