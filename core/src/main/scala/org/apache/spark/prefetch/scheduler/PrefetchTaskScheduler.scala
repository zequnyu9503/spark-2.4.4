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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.prefetch.{PrefetchOffer, PrefetchTaskDescription, SinglePrefetchTask}
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, HDFSCacheTaskLocation, TaskLocality}
import org.apache.spark.scheduler.TaskLocality.TaskLocality

class PrefetchTaskScheduler(
    offers: Seq[PrefetchOffer],
    hostToExecutors: mutable.HashMap[String, ArrayBuffer[String]],
    pTasks: Seq[SinglePrefetchTask[_]])
    extends Logging {

  private val env = SparkEnv.get
  private val ser = env.closureSerializer.newInstance()

  private val cores_exe = env.conf.getInt("cores.prefetch.executors", 2)

  private val forExecutors =
    new mutable.HashMap[String, ArrayBuffer[SinglePrefetchTask[_]]]()
  private val forHosts =
    new mutable.HashMap[String, ArrayBuffer[SinglePrefetchTask[_]]]()
  private val forNoRefs = new ArrayBuffer[SinglePrefetchTask[_]]()
  private val forAll = new ArrayBuffer[SinglePrefetchTask[_]]()

  private val isScheduledforTasks =
    new mutable.HashMap[SinglePrefetchTask[_], Boolean]()

  def isAllScheduled: Boolean = {
    !isScheduledforTasks.exists(_._2.equals(false))
  }

  addPendingTasks()

  private def addPendingTasks(): Unit = {
    for (i <- pTasks.indices) {
      for (loc <- pTasks(i).locs) {
        loc match {
          case exe: ExecutorCacheTaskLocation =>
            // which means partition located on running executors.
            forExecutors.getOrElseUpdate(exe.executorId,
              new ArrayBuffer[SinglePrefetchTask[_]]()) += pTasks(i)
          case hdfs: HDFSCacheTaskLocation =>
            // Find executors which hold cached data.
            val executors = hostToExecutors(hdfs.host)
            if (executors.nonEmpty) {
              executors.foreach { e => forExecutors.getOrElseUpdate(
                  e, new ArrayBuffer[SinglePrefetchTask[_]]()) += pTasks(i)
              }
            } else {
              logError(s"Task [${pTasks(i).taskId}] preferred Executor lost.")
            }
          case _ => // Nothing to do.
        }
        forHosts.getOrElseUpdate(loc.host, new ArrayBuffer[SinglePrefetchTask[_]]()) += pTasks(i)
        if (pTasks(i).locs == Nil) {
          forNoRefs += pTasks(i)
        }
        forAll += pTasks(i)
      }
      isScheduledforTasks(pTasks(i)) = false
    }
  }

  private def computeValidLocalityLevels(): Array[TaskLocality] = {
    import TaskLocality.{ANY, NODE_LOCAL, NO_PREF, PROCESS_LOCAL}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (forExecutors.nonEmpty) levels += PROCESS_LOCAL
    if (forHosts.nonEmpty) levels += NODE_LOCAL
    if (forNoRefs.nonEmpty) levels += NO_PREF
    if (forAll.nonEmpty) levels += ANY
    logInfo(s"Prefetch levels are ${levels.mkString(", ")} .")
    levels.toArray
  }

  private def resourceOffer(executorId: String,
                            host: String,
                            maxLocality: TaskLocality.TaskLocality)
    : Option[PrefetchTaskDescription] = {
    dequeueTask(executorId, host, maxLocality) match {
      case Some(blend) =>
        val desc = new PrefetchTaskDescription(executorId, blend._1.taskId, ser.serialize(blend._1))
        desc.locality = blend._2
        Some(desc)
      case _ => None
    }
  }

  private def dequeueTask(executorId: String, host: String,
                          maxLocality: TaskLocality.TaskLocality)
    : Option[(SinglePrefetchTask[_], TaskLocality.Value)] = {
    for (task <- dequeueTaskFromList(executorId,
           host, forExecutors.getOrElse(executorId, ArrayBuffer()))) {
      return Some((task, TaskLocality.PROCESS_LOCAL))
    }
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (task <- dequeueTaskFromList(
             executorId, host, forHosts.getOrElse(host, ArrayBuffer()))) {
        return Some((task, TaskLocality.NODE_LOCAL))
      }
    }
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NO_PREF)) {
      for (task <- dequeueTaskFromList(executorId, host, forNoRefs)) {
        return Some((task, TaskLocality.NO_PREF))
      }
    }
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.ANY)) {
      for (task <- dequeueTaskFromList(executorId, host, forAll)) {
        return Some((task, TaskLocality.ANY))
      }
    }
    None
  }

  private def dequeueTaskFromList(
      executorId: String,
      host: String,
      tasks: ArrayBuffer[SinglePrefetchTask[_]]): Option[SinglePrefetchTask[_]] = {
    var index = tasks.size
    while (index > 0) {
      index -= 1
      val task = tasks(index)
      if (!isScheduledforTasks(task)) {
        isScheduledforTasks(task) = true
        return Option(task)
      }
    }
    None
  }

  protected[prefetch] def makeResources(): Array[Array[PrefetchTaskDescription]] = {
    val descriptions = new ArrayBuffer[Array[PrefetchTaskDescription]]()
    var subDesc = new ArrayBuffer[PrefetchTaskDescription]()
    val maxLocalityLevels = computeValidLocalityLevels()
    var tasksPerExe = 0
    while (!isAllScheduled) {
      // Until All tasks are scheduled.
      for (taskLocality <- maxLocalityLevels) {
        for (offer <- offers) {
          // Find fittest tasks launched on every executor.
          resourceOffer(offer.executorId, offer.host, taskLocality) match {
            case Some(desc) => subDesc += desc
            case _ => logError("No resource matched.")
          }
        }
        tasksPerExe += 1

        if (tasksPerExe % cores_exe == 0) {
          descriptions += subDesc.toArray
          subDesc = new ArrayBuffer[PrefetchTaskDescription]()
        }
      }
    }
    descriptions.toArray
  }
}
