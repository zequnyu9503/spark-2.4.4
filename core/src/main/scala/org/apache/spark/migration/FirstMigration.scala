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
package org.apache.spark.migration

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.BlockId

class FirstMigration(
                      // blockId, executorId
                     val partitions: mutable.HashMap[BlockId, String],
                      // blockId, computation duration
                      val recompute: mutable.HashMap[BlockId, Long],
                     // blockId, cached size
                     val mem: mutable.HashMap[BlockId, Long],
                      val load: Long,
                      val deser: Long,
                      val next: Long) {

  private [migration] var MAX_MEM: Long = 8 * 1024 * 1024

  // Mappings from executors to blocks. One-to-many.
  private [migration] val executors = mutable.HashMap[String, List[BlockId]](partitions.
    groupBy(_._2).collect { case (exeId, bIds) =>
    (exeId, bIds.keys.toList)}.toArray: _*)

  private [migration] def isTriggered(hm: mutable.HashMap[String, Long]): Boolean =
    hm.values.sum > MAX_MEM

  private [migration] def reload(size: Long): Long = size * (load + deser)

  private [migration] def pickEachMax(exeId: String): Option[BlockId] = {
    Option(executors(exeId).filter(p => {
      val duration = reload(mem(p))
      duration < recompute(p) && duration <= next
    }).map(p => (p, mem(p))).maxBy(_._2)._1) match {
      case Some(bId) => Option(bId)
      case _ => None
    }
  }

  private [migration] def migrate(): ArrayBuffer[MigrationPlan] = {
    // Used for recording migrations.
    val plans: ArrayBuffer[MigrationPlan] = new ArrayBuffer[MigrationPlan]()

    // Memory used of all partitions per executor.
    val workload = executors.collect {
      case (executorId, blockIds) => (executorId, blockIds.map(id => mem(id)).sum)
    }

    // Whether get enough time left for migration
    var keepGoing = true

    while (keepGoing && isTriggered(workload)) {
      workload.keySet.foreach(exeId => {
        pickEachMax(exeId) match {
          case Some(bId) => plans += MigrationPlan(isLocal = true, isMem = false, exeId, bId, null)
        }
      })
    }

    plans
  }
}
