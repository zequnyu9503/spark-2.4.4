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

package org.apache.spark.prefetch.slave

import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.prefetch.{PrefetcherId, PrefetchTaskDescription, PrefetchTaskRunner}
import org.apache.spark.prefetch.PrefetchMessage.RegisterPrefetcher
import org.apache.spark.prefetch.master.PrefetcherMaster
import org.apache.spark.rpc.{RpcEndpointRef, RpcEnv}
import org.apache.spark.util.UninterruptibleThread

class Prefetcher(private val rpcEnv: RpcEnv,
                 private val executorId: String,
                 private val host: String,
                 private val port: Int,
                 val master: PrefetcherMaster,
                 val masterEndpoint: RpcEndpointRef)
    extends Logging {

  private var env_ : SparkEnv = _
  private var appId_ : String = _

  private var theadpoolexecutor_ : ThreadPoolExecutor = _

  // Unique Id for every prefetcher.
  private var prefetcherId: PrefetcherId = _

  // Initialize Endpoint and EndpointRef.
  private val rpcEndpoint = new PrefetcherEndpoint(rpcEnv, this)
  private val rpcEndpointRef =
    rpcEnv.setupEndpoint(Prefetcher.ENDPOINT_NAME(executorId), rpcEndpoint)

  def initialize(env: SparkEnv): Unit = {
    env_ = env
    if (prefetcherId.eq(null)) {
      logInfo(s"@YZQ Executor ${executorId} register prefetcher to master.")
      val pid = new PrefetcherId(executorId, host, port)
      prefetcherId = masterEndpoint.askSync[PrefetcherId](
        RegisterPrefetcher(pid, rpcEndpointRef)
      )
    }
    if (!prefetcherId.eq(null) && theadpoolexecutor_.eq(null)) {
      theadpoolexecutor_ = initializeThreadPoolExecutor()
    }
  }

  private def initializeThreadPoolExecutor(): ThreadPoolExecutor = {
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
    val taskRunner = new PrefetchTaskRunner(env_, taskDesc)
    theadpoolexecutor_.execute(taskRunner)
  }
}

object Prefetcher {

  def ENDPOINT_NAME(executorId: String): String = "Prefetcher-" + executorId
}
