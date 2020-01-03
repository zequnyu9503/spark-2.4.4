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

import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.SerializableBuffer

object PrefetchMessage {

  // Message from slaves to master.
  sealed trait Slave2Master

  case class RegisterPrefetcher(prefetcherId: PrefetcherId,
                                rpcEndpointRef: RpcEndpointRef)
      extends Slave2Master

  // Message from master to slaves.
  sealed trait MasterSlave2

  case class LaunchPrefetchTask(data: SerializableBuffer) extends MasterSlave2
}