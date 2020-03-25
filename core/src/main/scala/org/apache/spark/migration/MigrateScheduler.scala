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

import org.apache.spark.internal.Logging

import scala.reflect.ClassTag
import org.apache.spark.scheduler.SchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.storage.RDDBlockId

class MigrateScheduler(val backend: SchedulerBackend) extends Logging{

  private val cgsb_ : CoarseGrainedSchedulerBackend = {
    backend match {
      case backend: CoarseGrainedSchedulerBackend =>
        val coarse = backend.asInstanceOf[CoarseGrainedSchedulerBackend]
        coarse.migrateScheduler(this)
        coarse
      case _ =>
        null
    }
  }

  def migrate[T: ClassTag](rddId: Int, partitionId: Int,
                           sourceId: String, destinationId: String): Unit = {
    val migration = Migration[T](RDDBlockId(rddId, partitionId), sourceId, destinationId)
    logInfo(s"Create a migration for block ${partitionId} from ${sourceId} to ${destinationId}")
    cgsb_.receiveMigration(migration)
  }
}
