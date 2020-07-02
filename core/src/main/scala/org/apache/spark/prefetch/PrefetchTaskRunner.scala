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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance

class PrefetchTaskRunner(prefetcher: Prefetcher, env: SparkEnv,
                         taskDesc: PrefetchTaskDescription)
    extends Runnable with Logging {

  val ser: SerializerInstance = env.closureSerializer.newInstance()

  override def run(): Unit = {
    Thread.currentThread().setPriority(10)
    try {
      // This time stamp includes deserialize process
      // as well as computation.
      val task = ser.deserialize[SinglePrefetchTask[Any]](taskDesc.serializedTask,
        Thread.currentThread.getContextClassLoader)
      val timeline = task.startTask(TaskContext.empty())
      val reporter = PrefetchReporter(prefetcher.executorId,
        task.taskId, timeline._1, timeline._2 - timeline._1)
      prefetcher.reportTaskFinished(reporter)
    } catch {
      case t: Throwable =>
        logError(s"Exception in  prefetching", t)
    }
  }

  def waitRunning(): Unit = synchronized {
    val waiting = Prefetcher.delay()
    if (waiting > 0) {
      logInfo(s"Waiting $waiting ms to start task.")
      this.wait(waiting)
    } else {
      logInfo(s"No need to wait for it $waiting.")
    }
  }
}
