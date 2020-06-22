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
package org.apache.spark.prefetch.scheduler

import scala.collection.mutable

import org.apache.spark.prefetch.{PrefetchTaskResult, StreamPrefetchDeployment}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend

class StreamPrefetchDeployManager(cgsb : CoarseGrainedSchedulerBackend,
                                  deployment: Array[Array[StreamPrefetchDeployment]],
                                  job: StreamPrefetchJob) {

  @volatile
  var runningTasks = new mutable.HashMap[String, PrefetchTaskResult]()

  private def gotBatches(): Boolean = synchronized {
    !runningTasks.exists(_._2 eq null)
  }

  private def configBatches(schedule: Array[StreamPrefetchDeployment]):
  Unit = synchronized {
    if (runningTasks.nonEmpty) runningTasks.clear()
    schedule.foreach(desc => synchronized {
      runningTasks(desc.taskId) = null
    })
  }

  private def updateJob(): Unit = synchronized {
    runningTasks.keys.foreach(tId => job.updateTaskById(tId, runningTasks(tId)))
  }

  def updatePrefetchTask(result: PrefetchTaskResult): Unit = {
    synchronized {
      runningTasks(result.taskId) = result
    }
    if (gotBatches()) keepWorking()
  }

  def execute(): Unit = {
    for (index <- deployment.indices) {
      val schedule = deployment(index)
      configBatches(schedule)
      cgsb.submitPrefetches(this, schedule)
      waiting()
      updateJob()
    }
  }

  def waiting(): Unit = synchronized {
    this.wait()
  }

  def keepWorking(): Unit = synchronized {
    this.notify()
  }

}
