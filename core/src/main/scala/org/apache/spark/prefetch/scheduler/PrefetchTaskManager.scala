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
import org.apache.spark.prefetch.{PrefetchTask, PrefetchTaskDescription}
import org.apache.spark.prefetch.PrefetchMessage.LaunchPrefetchTask
import org.apache.spark.prefetch.master.PrefetcherMaster
import org.apache.spark.scheduler.{
  ExecutorCacheTaskLocation,
  HDFSCacheTaskLocation,
  SchedulerBackend,
  TaskLocality,
  TaskScheduler
}
import org.apache.spark.scheduler.TaskLocality.TaskLocality
import org.apache.spark.util.SerializableBuffer

class PrefetchTaskManager(val master: PrefetcherMaster,
                          ts: TaskScheduler,
                          val backend: SchedulerBackend,
                          val tasks: Seq[PrefetchTask[_]])
    extends Logging {

  val env = SparkEnv.get
  val ser = env.closureSerializer.newInstance()

  private val forExecutors =
    new mutable.HashMap[String, ArrayBuffer[PrefetchTask[_]]]()
  private val forHosts =
    new mutable.HashMap[String, ArrayBuffer[PrefetchTask[_]]]()
  private val forNoRefs = new ArrayBuffer[PrefetchTask[_]]()
  private val forAll = new ArrayBuffer[PrefetchTask[_]]()

  initialize()

  private def initialize(): Unit = {
    addPendingTasks()
  }

  private def addPendingTasks(): Unit = {
    for (i <- tasks.indices) {
      for (loc <- tasks(i).locs) {
        loc match {
          case exe: ExecutorCacheTaskLocation =>
            // which means partition located on running executors.
            forExecutors.getOrElseUpdate(
              exe.executorId,
              new ArrayBuffer[PrefetchTask[_]]()) += tasks(i)
          case hdfs: HDFSCacheTaskLocation =>
            master.hostToExecutors(hdfs.host) match {
              case Some(set) =>
                set.foreach(
                  e =>
                    forExecutors.getOrElseUpdate(
                      e,
                      new ArrayBuffer[PrefetchTask[_]]()) += tasks(i))
              case None =>
                logInfo(
                  s"Pending task has a location at" +
                    s"${hdfs.host} but no executors found")
            }
          case _ =>
        }
        forHosts.getOrElseUpdate(loc.host, new ArrayBuffer[PrefetchTask[_]]()) += tasks(
          i)
        if (tasks(i).locs == Nil) forNoRefs += tasks(i)
        forAll += tasks(i)
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
    levels.toArray
  }

  private def resourceOffer(executorId: String,
                            host: String,
                            maxLocality: TaskLocality.TaskLocality)
    : Option[PrefetchTaskDescription] = {
    dequeueTask(executorId, host, maxLocality) match {
      case Some(blend) =>
        Some(new PrefetchTaskDescription(executorId, ser.serialize(blend._1)))
      case _ => None
    }
  }

  private def dequeueTask(executorId: String,
                          host: String,
                          maxLocality: TaskLocality.TaskLocality)
    : Option[(PrefetchTask[_], TaskLocality.Value)] = {
    for (task <- dequeueTaskFromList(executorId,
                                     host,
                                     forExecutors(executorId))) {
      return Some((task, TaskLocality.PROCESS_LOCAL))
    }
    if (TaskLocality.isAllowed(maxLocality, TaskLocality.NODE_LOCAL)) {
      for (task <- dequeueTaskFromList(executorId, host, forHosts(executorId))) {
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

  private def makeOffer(): Array[PrefetchTaskDescription] = {
    val descriptions = new ArrayBuffer[PrefetchTaskDescription]()
    val maxLocalityLevels = computeValidLocalityLevels()
    for (currentMaxLocality <- maxLocalityLevels) {
      for (prefetcher <- master.prefetchList) {
        descriptions += resourceOffer(prefetcher.executorId,
                                      prefetcher.host,
                                      currentMaxLocality).get
      }
    }
    descriptions.toArray
  }

  def launchTasks(): Unit = {
    val taskDescs = makeOffer()
    if (taskDescs.nonEmpty) {
      for (task <- taskDescs) {
        val serializedTask = PrefetchTaskDescription.encode(task)
        master.executorToPrefetcher(task.executorId) match {
          case Some(prefetcherId) =>
            master
              .prefetcherEndpointList(prefetcherId)
              .send(LaunchPrefetchTask(new SerializableBuffer(serializedTask)))
          case _ => logError(s"No prefetcher on executor ${task.executorId}")
        }
      }
    }
  }
}
