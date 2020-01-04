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

import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.util.UninterruptibleThread

class Prefetcher(val executorId: String, val executorHostname: String)
    extends Logging {

  logInfo(s"Starting prefetcher ${executorId} on host ${executorHostname}")

  private val theadpoolexecutor_ : ThreadPoolExecutor = {
    val threadFactory = new ThreadFactoryBuilder()
      .setDaemon(false)
      .setNameFormat("Prefetch task launch p-%d")
      .setThreadFactory(new ThreadFactory {
        override def newThread(r: Runnable): Thread =
          new UninterruptibleThread(r, "unused")
      })
      .build()
    Executors.newCachedThreadPool(threadFactory).asInstanceOf[ThreadPoolExecutor]
  }

  def acceptLaunchTask(taskDesc: PrefetchTaskDescription): Unit = {
    val taskRunner = new PrefetchTaskRunner(SparkEnv.get, taskDesc)
    theadpoolexecutor_.execute(taskRunner)
  }
}
