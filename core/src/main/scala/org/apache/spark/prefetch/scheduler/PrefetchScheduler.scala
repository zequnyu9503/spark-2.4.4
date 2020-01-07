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

import java.io.NotSerializableException

import scala.collection.{mutable, Map}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, SparkContext, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.prefetch.{PrefetchOffer, PrefetchReporter, PrefetchTaskDescription, SinglePrefetchTask}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{DAGScheduler, SchedulerBackend, TaskLocation, TaskScheduler}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.StorageLevel

class PrefetchScheduler(val sc: SparkContext,
                        val backend: SchedulerBackend,
                        val ts: TaskScheduler,
                        val dag: DAGScheduler)
    extends Logging {

  logInfo("Initialize PrefetchScheduler.")

  private var prefetchJob_ : PrefetchJob = _

  private val cgsb_ : CoarseGrainedSchedulerBackend = {
    backend match {
      case backend: CoarseGrainedSchedulerBackend =>
        val coarse = backend.asInstanceOf[CoarseGrainedSchedulerBackend]
        coarse.prefetchScheduler(this)
        coarse
      case _ =>
        null
    }
  }

  // core function.
  def prefetch(rdd: RDD[_], callback: Seq[PrefetchReporter] => Unit = null): Unit = {
    if (!prefetchJob_.eq(null)) {
      logError(s"There is a prefetchJob running for RDD[${prefetchJob_.rdd.name}].")
      return
    }
    val pTasks = createPrefetchTasks(rdd.persist(StorageLevel.MEMORY_ONLY))
    if (pTasks.nonEmpty) {
      val tasks = new mutable.HashMap[SinglePrefetchTask[_], PrefetchReporter]()
      pTasks.foreach(e => tasks(e) = null)
      prefetchJob_ = new PrefetchJob(rdd, tasks, callback)
      logInfo(s"Create prefetch job include ${prefetchJob_.count} tasks.")
    } else {
      logInfo("Failed to create prefetch job for 0 task.")
      prefetchJob_ = null
      return
    }
    if (!cgsb_.eq(null)) {
      cgsb_.receivePrefetches(this)
    }
  }

  private def createPrefetchTasks(rdd: RDD[_]): Seq[SinglePrefetchTask[_]] = {
    var taskBinary: Broadcast[Array[Byte]] = null
    val partitions: Array[Partition] = rdd.partitions
    var taskBinaryBytes: Array[Byte] = null

    try {
      taskBinaryBytes =
        JavaUtils.bufferToArray(PrefetchScheduler.closureSerializer.serialize(rdd: AnyRef))
    } catch {
      case _ : NotSerializableException =>
        logError("NotSerializableException for RDD.")
        return Seq()
    }
    taskBinary = sc.broadcast(taskBinaryBytes)
    // Find preferring locations for each partition.
    val taskIdToLocations: Map[Partition, Seq[TaskLocation]] = partitions.map(
      partition => (partition, dag.getPreferredLocs(rdd, partition.index))
    ).toMap
    // Create prefetch tasks waiting to be launched.
   partitions.map(partition =>
      new SinglePrefetchTask(taskBinary, partition, taskIdToLocations(partition))).toSeq
  }

  protected [spark] def resourceOffers(offers: Seq[PrefetchOffer]):
  Array[PrefetchTaskDescription] = {
    if (prefetchJob_.eq(null)) return Array()
    val hostToExecutors = new mutable.HashMap[String, ArrayBuffer[String]]()
    for (o <- offers) {
      hostToExecutors.getOrElseUpdate(o.host, new ArrayBuffer[String]()) += o.executorId
    }
    val prefetchTaskManager = new PrefetchTaskManager(offers,
      hostToExecutors, prefetchJob_.tasks.keys.toSeq)
    prefetchTaskManager.makeResources()
  }

  protected [spark] def markPrefetchTaskFinished(reporter: PrefetchReporter): Unit = {
    if (!prefetchJob_.eq(null)) {
      prefetchJob_.updateTaskStatusById(reporter.taskId, reporter)
    }
    if (prefetchJob_.isAllFinished) {
      if (!prefetchJob_.callback.eq(null)) {
        // Execute function of callback.
        prefetchJob_.callback(prefetchJob_.tasks.values.toSeq)
      }
      prefetchJob_ = null
    }
  }
}

object PrefetchScheduler {
  def closureSerializer: SerializerInstance = SparkEnv.get.closureSerializer.newInstance()
}
