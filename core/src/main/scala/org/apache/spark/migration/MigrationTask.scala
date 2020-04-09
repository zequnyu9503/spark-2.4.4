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

import org.apache.spark.SparkEnv
import org.apache.spark.executor.{DataReadMethod, ExecutorBackend}
import org.apache.spark.internal.Logging
import org.apache.spark.storage.{BlockResult, MigrationHelper}

class MigrationTask[T](val executorId: String, val env: SparkEnv,
                                 val backend: ExecutorBackend,
                                 val migrationHelper: MigrationHelper,
                                 val migration: Migration[T]) extends Runnable with Logging{

  override def run(): Unit = {
    if (migration.isMem) migrateToMem()
    if (migration.isLocal) migrateToDisk()
  }

  private def getRemoteMemAsIterator[T](migration: Migration[T]): Iterator[T] = {
    val remote = env.blockManager.getRemoteBytes(migration.blockId)
    if (remote.isEmpty) {
      logError(s"Migrate block [${migration.blockId}] failed")
      return null
    }

    remote.map { data =>
      val values = env.serializerManager.dataDeserializeStream(
        migration.blockId, data.toInputStream(dispose = true))(migration.elementClassTag)
      new BlockResult(values, DataReadMethod.Network, data.size)
    } match {
      case Some(blockResult) => blockResult.data.asInstanceOf[Iterator[T]]
      case _ => null
    }
  }

  private def getLocalMemAsIterator[T](migration: Migration[T]): Iterator[T] = {
    env.blockManager.getLocalValues(migration.blockId) match {
      case Some(blockResult) =>
        logInfo(s"Found local block [${migration.blockId}] in memory.")
        blockResult.data.asInstanceOf[Iterator[T]]
      case _ => null
    }
  }

  private def migrateToMem(): Unit = {
    val size = migrationHelper.putIteratorAsMemValue[T](migration.blockId,
      getRemoteMemAsIterator(migration), migration.elementClassTag)
    migrationHelper.reportBlockCachedInMem(migration.blockId, size)

    val newMigration = Migration(migration.isLocal, migration.isMem,
      migration.blockId, migration.sourceId, migration.destinationId,
      migration.isSourceFinished, isDestinationFinished = true)
    migrationHelper.reportDestinationToExecutor(newMigration)
  }

  private def migrateToDisk(): Unit = {
    val size = migrationHelper.putIteratorIntoDisk(migration.blockId,
      getLocalMemAsIterator(migration), migration.elementClassTag)
    val newMigration = if (migrationHelper.updateAndReportForDisk(migration, size)) {
       Migration(migration.isLocal, migration.isMem,
        migration.blockId, migration.sourceId, migration.destinationId,
        isSourceFinished = true, isDestinationFinished = true)
    } else {
      migration
    }
    migrationHelper.reportDestinationToExecutor(newMigration)
  }
}
