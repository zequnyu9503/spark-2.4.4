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
import org.apache.spark.prefetch.{PrefetchOffer, PrefetchReporter, PrefetchTaskDescription, SinglePrefetchTask, StorageMemory}
import org.apache.spark.prefetch.cluster.PrefetchPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{DAGScheduler, SchedulerBackend, TaskLocation, TaskScheduler}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.storage.{BlockId, BlockManagerMaster, RDDBlockId, StorageLevel}

class PrefetchScheduler(val sc: SparkContext,
                        val backend: SchedulerBackend,
                        val ts: TaskScheduler,
                        val dag: DAGScheduler)
    extends Logging {

  logInfo("Initialize PrefetchScheduler.")

  private val cgsb_ : CoarseGrainedSchedulerBackend = {
    backend match {
      case backend: CoarseGrainedSchedulerBackend =>
        backend.asInstanceOf[CoarseGrainedSchedulerBackend]
      case _ =>
        null
    }
  }

  // core function.
  def prefetch(rdd: RDD[_]): Option[Seq[PrefetchReporter]] = {
    createPrefetchJob(rdd) match {
      case Some(job) =>
        logInfo(s"Create prefetch job for rdd [${rdd.name}].")
        val offers = makePrefetchOffers()
        logInfo(s"Make resources [${offers.size} exes] for prefetch job.")
        job.schedules = makeSchedules(job, offers)
        logInfo(s"Make schedules [${job.schedules.length} batches] for prefetch job.")
        val taskManager = new PrefetchTaskManager(cgsb_, job)
        taskManager.execute()
        Option(job.gotReporter())
      case None => None
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

  private def createPrefetchJob(rdd: RDD[_]): Option[PrefetchJob] = {
    val pTasks = createPrefetchTasks(rdd.persist(StorageLevel.MEMORY_ONLY))
    logInfo(s"Create prefetch tasks [${pTasks.size}] for rdd [${rdd.name}]")
    if (pTasks.nonEmpty) {
      val tasks = new mutable.HashMap[SinglePrefetchTask[_], PrefetchReporter]()
      // Initialize prefetch job.
      pTasks.foreach(e => tasks(e) = null)
      Option(new PrefetchJob(rdd, tasks))
    } else {
      logInfo("Failed to create prefetch job for 0 task.")
      None
    }
  }

  private def makePrefetchOffers(): Seq[PrefetchOffer] = {
    // Under normal case we should check alive executors firstly.
    val exeData = cgsb_.retrieveExeDataForPrefetch
    while (exeData.isEmpty) {
      wait(100)
    }
    exeData.map {
      case (id, executorData) =>
        PrefetchOffer(id, executorData.executorHost)
    }.toSeq
  }

  private def makeSchedules(jobs: PrefetchJob, offers: Seq[PrefetchOffer]):
  Array[Array[PrefetchTaskDescription]] = {
    val hostToExecutors = new mutable.HashMap[String, ArrayBuffer[String]]()
    for (o <- offers) {
      hostToExecutors.getOrElseUpdate(o.host, new ArrayBuffer[String]()) += o.executorId
    }
    val pts = new PrefetchTaskScheduler(offers,
      hostToExecutors, jobs.tasks.keys.toSeq)
    pts.makeResources()
  }

  // Synchronize process.
  def freeStorageMemory(): mutable.HashMap[String, Long] = {
    val offers = makePrefetchOffers()
    val storageMemory = StorageMemory(offers.size, mutable.HashMap.empty)
    cgsb_.retrieveFreeStorageMemory(offers, storageMemory)

    while (!storageMemory.isDone) {
      synchronized {
        wait(100)
      }
    }

    storageMemory.free
  }

  def sizeInMem(rdd: RDD[_]): Long = {
    var memSize: Long = 0L
    val bmm = SparkEnv.get.blockManager.master
    rdd.partitions.foreach(partition => {
      val blockId = RDDBlockId(rdd.id, partition.index)
      bmm.getLocationsAndStatus(blockId) match {
        case Some(status) => memSize += status.status.memSize
      }
    })
    memSize
  }

  def makePlan(winId: Int, rdd: RDD[_]): Option[PrefetchPlan] = {
    val plan = new PrefetchPlan(winId, rdd)
    createPrefetchJob(rdd) match {
      case Some(job) =>
        val offers = makePrefetchOffers()
        plan.schedule = makeSchedules(job, offers)
        Some(plan)
      case None => None
    }
  }

}

object PrefetchScheduler {
  def closureSerializer: SerializerInstance = SparkEnv.get.closureSerializer.newInstance()
}
