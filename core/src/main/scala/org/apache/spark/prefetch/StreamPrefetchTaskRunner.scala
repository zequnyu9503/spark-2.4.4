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

import org.apache.spark.internal.Logging

class StreamPrefetchTaskRunner(prefetcher: Prefetcher, meta: StreamMeta)
  extends Runnable with Logging {

  override def run(): Unit = {
    val task = new StreamPrefetchTask(prefetcher.blockManager)
    task.startPrefetchTask(meta) match {
      case Some(duration) =>
        val partitionId = meta.partition.partition.index
        val result = PrefetchTaskResult(prefetcher.executorId, partitionId.toString, partitionId,
          duration._1, duration._2)
        prefetcher.backPrefetchTaskResult(result)
      case _ => prefetcher.backPrefetchTaskResult(null)
    }
  }

  private def waitRunning(): Unit = synchronized {
    val waiting = Prefetcher.delay()
    if (waiting > 0) {
      logInfo(s"Waiting $waiting ms to launch prefetch deployment].")
      this.wait(waiting)
    } else {
      logInfo(s"No need to wait for it $waiting.")
    }
  }
}
