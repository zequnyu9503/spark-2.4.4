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
import org.apache.spark.prefetch.{PrefetchOffer, PrefetchTask, PrefetchTaskDescription}
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, HDFSCacheTaskLocation, TaskLocality}
import org.apache.spark.scheduler.TaskLocality.TaskLocality

class PrefetchTaskManager(
    offers: Seq[PrefetchOffer],
    hostToExecutors: mutable.HashMap[String, ArrayBuffer[String]],
    pTasks: Seq[PrefetchTask[_]])
    extends Logging {

  private val env = SparkEnv.get
  private val ser = env.closureSerializer.newInstance()

  private val forExecutors =
    new mutable.HashMap[String, ArrayBuffer[PrefetchTask[_]]]()
  private val forHosts =
    new mutable.HashMap[String, ArrayBuffer[PrefetchTask[_]]]()
  private val forNoRefs = new ArrayBuffer[PrefetchTask[_]]()
  private val forAll = new ArrayBuffer[PrefetchTask[_]]()

  addPendingTasks()

  private def addPendingTasks(): Unit = {
    for (i <- pTasks.indices) {
      for (loc <- pTasks(i).locs) {
        loc match {
          case exe: ExecutorCacheTaskLocation =>
            // which means partition located on running executors.
            forExecutors.getOrElseUpdate(
              exe.executorId,
              new ArrayBuffer[PrefetchTask[_]]()) += pTasks(i)
            logInfo(
              s"Task [${pTasks(i).taskId}] added into forExecutors as ${exe.executorId}.")
          case hdfs: HDFSCacheTaskLocation =>
            val executors = hostToExecutors(hdfs.host)
            if (executors.nonEmpty) {
              executors.foreach { e =>
                forExecutors.getOrElseUpdate(
                  e,
                  new ArrayBuffer[PrefetchTask[_]]()) += pTasks(i)
                logInfo(
                  s"Task [${pTasks(i).taskId}] added into forExecutors as ${e}")
              }
            } else {
              logError(s"Task [${pTasks(i).taskId}] preferred Executor lost.")
            }
          case _ =>
        }
        forHosts.getOrElseUpdate(loc.host, new ArrayBuffer[PrefetchTask[_]]()) += pTasks(
          i)
        logInfo(
          s"Task [${pTasks(i).taskId}] added into forHosts as ${loc.host}")
        if (pTasks(i).locs == Nil) {
          forNoRefs += pTasks(i)
          logInfo(s"Task [${pTasks(i).taskId}] added into forNoRefs")
        }
        forAll += pTasks(i)
        logInfo(s"Task [${pTasks(i).taskId}] added into forAll")
      }
    }
  }

  private def computeValidLocalityLevels(): Array[TaskLocality] = {
    import TaskLocality.{PROCESS_LOCAL, NODE_LOCAL, NO_PREF, ANY}
    val levels = new ArrayBuffer[TaskLocality.TaskLocality]
    if (forExecutors.nonEmpty) levels += PROCESS_LOCAL
    if (forHosts.nonEmpty) levels += NODE_LOCAL
    if (forNoRefs.nonEmpty) levels += NO_PREF
    if (forAll.nonEmpty) levels += ANY
    logInfo(s"Prefetch levels are ${levels.mkString(", ")}")
    levels.toArray
  }

  private def resourceOffer(executorId: String,
                            host: String,
                            maxLocality: TaskLocality.TaskLocality)
    : Option[PrefetchTaskDescription] = {
    dequeueTask(executorId, host, maxLocality) match {
      case Some(blend) =>
        logInfo(
          s"Offer resource for ${blend._1.taskId}" +
            s"on executor ${executorId} belongs to host ${host}")
        Some(new PrefetchTaskDescription(executorId, ser.serialize(blend._1)))
      case _ => None
    }
  }

  private def dequeueTask(executorId: String,
                          host: String,
                          maxLocality: TaskLocality.TaskLocality)
    : Option[(PrefetchTask[_], TaskLocality.Value)] = {
    logInfo(s"ExecutorId=${executorId} host=${host}")
    for (task <- dequeueTaskFromList(executorId,
                                     host,
                                     forExecutors.getOrElse(executorId, ArrayBuffer()))) {
      return Some((task, TaskLocality.PROCESS_LOCAL))
    }
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (task <- dequeueTaskFromList(executorId, host, forHosts.getOrElse(host, ArrayBuffer()))) {
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
      tasks: ArrayBuffer[PrefetchTask[_]]): Option[PrefetchTask[_]] = {
    var index = tasks.size
    while (index > 0) {
      index -= 1
      return Option(tasks(index))
    }
    None
  }

  protected[prefetch] def makeResources(): Array[PrefetchTaskDescription] = {
    val descriptions = new ArrayBuffer[PrefetchTaskDescription]()
    // It's upper limit.
    val maxLocalityLevels = computeValidLocalityLevels()
    for (currentMaxLocality <- maxLocalityLevels) {
      for (offer <- offers) {
        // Find fittest tasks launched on every executor.
        resourceOffer(offer.executorId, offer.host, currentMaxLocality) match {
          case Some(descs) => descriptions += descs
        }
      }
    }
    descriptions.toArray
  }
}
