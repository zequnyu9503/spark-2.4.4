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

import org.apache.spark.internal.Logging
import org.apache.spark.storage.memory.MemoryStore

private [spark] class MigrationHelper(blockManager: BlockManager,
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

  private [spark] def reportToMaster(blockId: BlockId, size: Long): Unit = {
     master.updateBlockInfo(blockManagerId, blockId,
       StorageLevel.MEMORY_ONLY, size, 0L)
    logInfo("Told master about the block cached for migration.")
   }
}
