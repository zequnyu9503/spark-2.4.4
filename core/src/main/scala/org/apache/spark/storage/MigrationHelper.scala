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
package org.apache.spark.storage

import scala.reflect.ClassTag

import org.apache.spark.executor.CoarseGrainedExecutorBackend
import org.apache.spark.internal.Logging
import org.apache.spark.migration.Migration
import org.apache.spark.storage.memory.MemoryStore

private [spark] class MigrationHelper(backend: CoarseGrainedExecutorBackend,
                                      blockManager: BlockManager,
                                      memoryStore: MemoryStore) extends Logging{

  private val master = blockManager.master
  private val blockManagerId = blockManager.blockManagerId

  private [spark] def putItreatorAsValue[T](blockId: BlockId,
                                       itr: Iterator[T], c: ClassTag[T]): Long = {
    memoryStore.putIteratorAsValues[T](blockId, itr, c) match {
      case Right(s) =>
        logInfo("Put iterator into memory successfully")
        s
      case Left(iter) =>
        logError("Put iterator failed for insufficient memory space.")
        0L
    }
  }

  private [spark] def reportDestinationToExecutor(blockId: BlockId, size: Long): Boolean = {
    logInfo("Telling (Sync) master that the block was cached on the executor.")
    master.updateBlockInfo(blockManagerId, blockId,
      StorageLevel.MEMORY_ONLY, size, 0L)
   }

  // Here we try to tell the source executor to remove the origin block
  // because the block has already been migrated to a new executor. It's
  // necessary to deliver messages through master.
  private [spark] def reportDestinationToExecutor(migration: Migration[_]): Unit = {
    backend.migrationFinished(migration)
  }

  private [spark] def removeReplicated(blockId: BlockId): Unit = {
    // default: tellMaster is true.
    blockManager.removeBlock(blockId)
  }

  private [spark] def reportSourceToExecutor[T: ClassTag](migration: Migration[T]): Unit = {
    val newMigration = Migration[T](migration.blockId, migration.sourceId, migration.destinationId,
      migration.source, true)
    backend.migrationFinished(newMigration)
  }
}
