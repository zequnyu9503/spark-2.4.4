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
import scala.reflect.ClassTag

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SchedulerBackend
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.storage.{BlockId, RDDBlockId}

class MigrateScheduler(val backend: SchedulerBackend) extends Logging {

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

  // Record all migrations we have ever handled.
  private val migrations = new mutable.LinkedHashMap[BlockId, Migration[_]]()

  def migrate[T: ClassTag](rddId: Int,
                           partitionId: Int,
                           sourceId: String,
                           destinationId: String): Unit = {
    val blockId = RDDBlockId(rddId, partitionId)
    if (migrations.contains(blockId) && migrations(blockId).started()) {
      // Which means Spark is working on migrations. Cancel migration request.
      logError(s"Block [${blockId.toString}] is being migrated at this moment.")
    } else {
      doMigration(isLocal = false, isMem = true, blockId, sourceId, destinationId)
    }
  }

  def migrate[T: ClassTag](rddId: Int, partitionId: Int, sourceId: String): Unit = {
    val blockId = RDDBlockId(rddId, partitionId)
    if (migrations.contains(blockId) && migrations(blockId).finished()) {
      // Which means Spark is working on migrations. Cancel migration request.
      logError(s"Block [${blockId.toString}] is being migrated at this moment.")
    } else {
      doMigration(isLocal = true, isMem = false, blockId, sourceId, sourceId)
    }
  }

  def migrate(plan: MigrationPlan): Unit = {}

  private[spark] def doMigration[T: ClassTag](isLocal: Boolean,
                                              isMem: Boolean,
                                              blockId: BlockId,
                                              sourceId: String,
                                              destinationId: String): Unit = {
    val migration = Migration[T](isLocal, isMem, blockId, sourceId, destinationId,
      isSourceFinished = false, isDestinationFinished = false)
    migrations(blockId) = migration
    cgsb_.receiveMigration(migration)
  }

  private[spark] def migrationUpdate[T: ClassTag](migration: Migration[T]): Unit = {
    if (migration.isMem) {
      if (migration.isDestinationFinished) {
        migrations(migration.blockId) = migration
        if (!migration.isSourceFinished) {
          logInfo("Pull block successfully, we then remove that replicated block.")
          cgsb_.receiveMigration(migration)
        } else {
          migrations(migration.blockId) = migration
          logInfo("Replicated block was successfully removed. Mem-Migration success.")
        }
      } else {
        migrations(migration.blockId) = MigrateScheduler.reset(migration)
        logError("Migrate block failed to pull blocks from others.")
      }
    }
    if (migration.isLocal) {
      if (migration.finished()) {
        migrations(migration.blockId) = migration
        logInfo("Disk-Migration success.")
      }
    }
  }
}

object MigrateScheduler {

  def reset[T: ClassTag](migration: Migration[T]): Migration[T] = {
    Migration(migration.isLocal, migration.isMem, migration.blockId,
      migration.sourceId, migration.destinationId, isSourceFinished = false,
      isDestinationFinished = false)
  }
}
