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
package org.apache.spark.prefetch.master

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.prefetch.{PrefetcherId, PrefetchTask}
import org.apache.spark.prefetch.PrefetchMessage.RegisterPrefetcher
import org.apache.spark.rpc.{RpcCallContext, RpcEnv, ThreadSafeRpcEndpoint}

class PrefetcherMasterEndpoint(override val rpcEnv: RpcEnv)
    extends ThreadSafeRpcEndpoint
    with Logging {

  // Initialize instance of master firstly.
  private var master_ : PrefetcherMaster = _

  private val finished = new mutable.HashMap[PrefetcherId, Boolean]()

  protected[prefetch] def setMaster(master: PrefetcherMaster): Unit =
    master_ = master

  override def receiveAndReply(
      context: RpcCallContext): PartialFunction[Any, Unit] = {
    // Accepty registration and reply it's prefetcherId.
    case RegisterPrefetcher(prefetcherId, rpcEndpointRef) =>
      context.reply(
        master_.acceptRegistration(prefetcherId.executorId,
                                   prefetcherId.host,
                                   prefetcherId.port,
                                   rpcEndpointRef))
  }

  def launchPrefetchTasks(pTasks: Seq[PrefetchTask[_]]): Unit = {
    for (i <- pTasks.indices) {
      val task: PrefetchTask[_] = pTasks(i)
      var found: Boolean = false
      for (location <- task.locs if !found) {
        if (location.)
      }
    }
  }

}
