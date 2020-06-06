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

  private val forExecutors =
    new mutable.HashMap[String, ArrayBuffer[SinglePrefetchTask[_]]]()
  private val forHosts =
    new mutable.HashMap[String, ArrayBuffer[SinglePrefetchTask[_]]]()
  private val forNoRefs = new ArrayBuffer[SinglePrefetchTask[_]]()
  private val forAll = new ArrayBuffer[SinglePrefetchTask[_]]()

  def isAllScheduled: Boolean = forAll.nonEmpty

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
    }
  }

  private def pickTaskFromOffer(offer: PrefetchOffer): Option[PrefetchTaskDescription] = {
    var task: SinglePrefetchTask[_] = null
    var maxLocality: TaskLocality = null
    if (task.eq(null) && forExecutors.nonEmpty && forExecutors.contains(offer.executorId)) {
      maxLocality = TaskLocality.PROCESS_LOCAL
      task = forExecutors(offer.executorId)(0).clone().asInstanceOf[SinglePrefetchTask[_]]
      forExecutors(offer.executorId).remove(0)
    }
    if (task.eq(null) && forHosts.nonEmpty && forHosts.contains(offer.executorId)) {
      maxLocality = TaskLocality.NODE_LOCAL
      task = forHosts(offer.host)(0).clone().asInstanceOf[SinglePrefetchTask[_]]
      forHosts(offer.host).remove(0)
    }
    if (task.eq(null) && forNoRefs.nonEmpty) {
      maxLocality = TaskLocality.NO_PREF
      task = forNoRefs(0)
      forNoRefs.remove(0)
    }
    if (task.eq(null) && forAll.nonEmpty) {
      maxLocality = TaskLocality.ANY
      task = forAll(0)
      forAll.remove(0)
    }
    if (!task.eq(null)) {
      val desc = new PrefetchTaskDescription(offer.executorId, task.taskId, ser.serialize(task))
      desc.locality = maxLocality
      Option(desc)
    } else None
  }

  protected[prefetch] def makeResources(cores_exe: Int): Array[Array[PrefetchTaskDescription]] = {
    val descriptions = new ArrayBuffer[Array[PrefetchTaskDescription]]()
    var subDesc = new ArrayBuffer[PrefetchTaskDescription]()
    while (!isAllScheduled) {
      var cores = 0
      while (cores < cores_exe) {
        for (offer <- offers) {
          pickTaskFromOffer(offer) match {
            case Some(desc) => subDesc += desc
            case None =>
          }
        }
        cores += 1
      }
      descriptions += subDesc.toArray
      cores = 0
    }
    descriptions.toArray
  }
}
