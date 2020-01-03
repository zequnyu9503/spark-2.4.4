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
import org.apache.spark.prefetch.PrefetcherId
import org.apache.spark.rpc.RpcEndpointRef

class PrefetcherMaster(var endpointRef: RpcEndpointRef,
                       val endpoint: PrefetcherMasterEndpoint)
    extends Logging {

  initialize()

  private val prefetcherList_ = new mutable.HashSet[PrefetcherId]()

  def prefetchList: mutable.HashSet[PrefetcherId] = prefetcherList_

  // Mapping from Prefetcher to RpcEndpointRef.
  private val prefetcherEndpointList_ =
    new mutable.HashMap[PrefetcherId, RpcEndpointRef]()

  def prefetcherEndpointList: mutable.HashMap[PrefetcherId, RpcEndpointRef] =
    prefetcherEndpointList_

  // PrefetcherMaster is disabled before initialize() is called.
  def initialize(): Unit = {
    endpoint.setMaster(this)
  }

  def hostToExecutors(host: String): Option[Set[String]] = {
    Option(prefetcherList_.filter(_.host.equals(host)).map(_.executorId).toSet)
  }

  def executorToPrefetcher(executorId: String): Option[PrefetcherId] = {
    prefetchList.find(_.executorId.equals(executorId))
  }

  def acceptRegistration(executorId: String,
                         host: String,
                         port: Int,
                         rpcEndpointRef: RpcEndpointRef): PrefetcherId = {
    val pid = new PrefetcherId(executorId, host, port)
    if (!prefetcherList_.exists(_.executorId.equals(pid.executorId))) {
      prefetcherList_.add(pid)
      prefetcherEndpointList_(pid) = rpcEndpointRef
      logInfo(
        s"@YZQ Accept registration of prefetcher ${pid.prefetcherId} on executor ${pid.executorId}")
    }
    pid
  }
}

object PrefetcherMaster {
  def ENDPOINT_NAME: String = "PrefetcherMaster"
}