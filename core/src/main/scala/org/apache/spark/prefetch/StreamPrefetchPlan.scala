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

import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.{BlockId, StorageLevel}

class StreamPrefetchPlan(blockId: BlockId,
                         streamPrefetchPartition: StreamPrefetchPartition,
                         locs_ : Seq[TaskLocation]) {

  val taskId: String = blockId.asRDDId match {
    case Some(rbId) => rbId.splitIndex.toString
    case _ => null
  }

  def locs: Seq[TaskLocation] = locs_

  def toMeta: StreamMeta =
    StreamMeta(blockId, StorageLevel.MEMORY_ONLY_SER, streamPrefetchPartition)
}
