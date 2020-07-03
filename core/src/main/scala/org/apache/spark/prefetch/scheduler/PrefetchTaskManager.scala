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

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable

import org.slf4j.LoggerFactory

import org.apache.spark.prefetch.{PrefetchReporter, PrefetchTaskDescription}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend


class PrefetchTaskManager(cgsb : CoarseGrainedSchedulerBackend,
                          job: PrefetchJob) {

  private val logger = LoggerFactory.getLogger("prefetch")

  @volatile
  var pendingTasks = new mutable.HashMap[String, PrefetchReporter]()

  private def gotBatches(): Boolean = synchronized {
    !pendingTasks.exists(_._2 eq null)
  }

  private def configBatches(schedule:
                            Array[PrefetchTaskDescription]): Unit = synchronized {
    if (pendingTasks.nonEmpty) pendingTasks.clear()
    schedule.foreach(desc => synchronized {
      pendingTasks(desc.taskId) = null
    })
  }

  private def updateJob(): Unit = synchronized {
    if (pendingTasks.nonEmpty) {
      val reporters = pendingTasks.values.toSeq.sortBy(_.startLine)
      val sdf = new SimpleDateFormat("hh:mm:ss")
      reporters.foreach(reporter => {
        job.updateTaskById(reporter.taskId, reporter)
        logger.info(s"Update prefetch [${reporter.taskId}] launch at" +
          s"{${sdf.format(new Date(reporter.startLine))} " +
          s"costs <${reporter.duration}> ms on executor (${reporter.eId}).")
      })
    }
  }

  def updatePrefetchTask(reporter: PrefetchReporter): Unit = {
    synchronized {
      pendingTasks(reporter.taskId) = reporter
    }
    if (gotBatches()) keepWorking()
  }

  def execute(): Unit = {
    val schedules = job.schedules
    for (index <- schedules.indices) {
      val schedule = schedules(index)
      configBatches(schedule)
      logger.info(s"Submit prefetch tasks to executors" +
        s" [${schedule.map(_.executorId).mkString(",")}]")
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
