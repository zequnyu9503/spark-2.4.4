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

class Migrant(val executorId: String, val executorHostname: String,
              val env: SparkEnv, val backend: ExecutorBackend)
    extends Logging {

  private val migrationHelper =
    new MigrationHelper(env.blockManager, env.blockManager.memoryStore)

  def acceptMigrant[T](migration: Migration[T]): Unit = {
    cacheBlock(migration, getIterator(migration))
  }

  private def getIterator[T](migration: Migration[T]): Iterator[T] = {
    val remote = env.blockManager.getRemoteBytes(migration.blockId)
    if (remote.isEmpty) {
      logError(s"Migrate block [${migration.blockId}] failed")
      return null;
    }

    remote.map { data =>
      val values = env.serializerManager.dataDeserializeStream(
        migration.blockId,
        data.toInputStream(dispose = true))(migration.elementClassTag)
      new BlockResult(values, DataReadMethod.Network, data.size)
    } match {
      case Some(blockResult) => blockResult.data.asInstanceOf[Iterator[T]]
      case _ => null
    }
  }

  private def cacheBlock[T](migration: Migration[T], itr: Iterator[T]): Unit = {
    val size = migrationHelper.putItreatorAsValue(migration.blockId,
                                                  itr, migration.elementClassTag)
    migrationHelper.reportToMaster(migration.blockId, size)
  }
}
