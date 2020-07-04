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
package org.apache.spark.prefetch

import scala.collection.mutable

import org.apache.spark.SparkEnv
import org.apache.spark.executor.{CoarseGrainedExecutorBackend, ExecutorBackend}
import org.apache.spark.internal.Logging


class Prefetcher(val executorId: String, val executorHostname: String, val backend: ExecutorBackend)
    extends Logging {

  val prefetcherId: String = executorId

  logInfo(s"Starting prefetcher [$prefetcherId] on host $executorHostname")

  def reportTaskFinished(reporter: PrefetchReporter): Unit = {
    backend match {
      case backend: CoarseGrainedExecutorBackend =>
        backend.prefetchTaskFinished(reporter)
      case _ => logError("Report failed for prefetching process.")
    }
  }

  def freeStorageMemory(): Unit = {
    val maxOnHeap = SparkEnv.get.memoryManager.maxOnHeapStorageMemory
    val maxOffHeap = SparkEnv.get.memoryManager.maxOffHeapStorageMemory
    val size = SparkEnv.get.memoryManager.storageMemoryUsed
    logInfo(s"Retrieve available storage memory both" +
      s"on heap [$maxOnHeap] & off heap [$maxOffHeap].")
    backend match {
      case backend: CoarseGrainedExecutorBackend =>
        backend.reportFreeStorageMemory(maxOffHeap + maxOnHeap - size)
      case _ => logError("Report failed for retrieving process.")
    }
  }
}

object Prefetcher {

  private val shuffles = new mutable.HashSet[Int]()

  def canFetch: Boolean = synchronized(shuffles.isEmpty)

  // We need to avoid reading conflict encountering with shuffle write.
  def startShuffleWrite(partitionId: Int): Unit = synchronized {
    if (!shuffles.contains(partitionId)) shuffles.add(partitionId)
  }

  def overShuffleWrite(partitionId: Int): Unit = synchronized {
    if (shuffles.contains(partitionId)) shuffles.remove(partitionId)
  }
}
