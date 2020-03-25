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

import scala.reflect.ClassTag

import org.apache.spark.SparkEnv
import org.apache.spark.executor.ExecutorBackend
import org.apache.spark.internal.Logging
import org.apache.spark.storage.MigrationHelper

class Migrant(val executorId: String, val executorHostname: String,
              val env: SparkEnv, val backend: ExecutorBackend)
    extends Logging {

  private val migrationHelper =
    new MigrationHelper(env.blockManager, env.blockManager.memoryStore)

  def acceptMigrant(migration: Migration[_]): Unit = {
    executorId match {
      case migration.sourceId =>
        val ct = migration.elementClassTag
        val migrationTask = new MigrationTask(executorId, env,
          backend, migrationHelper, migration)
        new Thread(migrationTask).start()
      case migration.destinationId =>
        migrationHelper.removeReplicated(migration.blockId)
        migrationHelper.reportSourceToExecutor(migration)
    }

  }


}
