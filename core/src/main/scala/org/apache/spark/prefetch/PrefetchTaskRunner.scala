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

import org.apache.spark.internal.Logging
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.serializer.SerializerInstance


class PrefetchTaskRunner(val prefetcher: Prefetcher,
                         val env: SparkEnv,
                         val taskDescription: PrefetchTaskDescription)
    extends Runnable
    with Logging {

  val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def run(): Unit = {
    try {
      // This time stamp includes deserialize process
      // as well as computation.
      val startTime = System.currentTimeMillis()
      val task = ser.deserialize[SinglePrefetchTask[Any]](
        taskDescription.serializedTask,
        Thread.currentThread.getContextClassLoader)
      val elements = task.startTask(TaskContext.empty())
      val endTime = System.currentTimeMillis()
      val reporter = PrefetchReporter(task.taskId, elements, endTime - startTime)
      prefetcher.reportTaskFinished(reporter)
    } catch {
      case t: Throwable =>
        logError(s"Exception in  prefetching", t)
    }
  }
}
