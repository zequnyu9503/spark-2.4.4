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

import org.apache.hadoop.mapred.FileSplit

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.prefetch.{PrefetchOffer, PrefetchTaskResult, StreamPrefetchDeployment, StreamPrefetchPartition, StreamPrefetchPlan}
import org.apache.spark.rdd.{HadoopPartition, HadoopRDD, RDD}
import org.apache.spark.scheduler.{DAGScheduler, SchedulerBackend, TaskLocation, TaskScheduler}
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.storage.RDDBlockId

class StreamPrefetchScheduler(val sc: SparkContext,
                              val backend: SchedulerBackend,
                              val ts: TaskScheduler,
                              val dag: DAGScheduler) {

  private val cores_exe = sc.getConf.getInt("cores.prefetch.executors", 4)

  private val cgsb_ : CoarseGrainedSchedulerBackend = {
    backend match {
      case backend: CoarseGrainedSchedulerBackend =>
        backend.asInstanceOf[CoarseGrainedSchedulerBackend]
      case _ =>
        null
    }
  }

  def prefetch(rdd: RDD[_]): Option[Array[PrefetchTaskResult]] = {
    val prefetched = findRoot(rdd).head
    val streamJob = createPrefetchJob(prefetched)
    val offers = makePrefetchOffers()
    val deployment = makeSchedules(streamJob, offers, cores_exe)
    val manager = new StreamPrefetchDeployManager(cgsb_, deployment, streamJob)
    manager.execute()
    None
  }

  private def findRoot(rdd: RDD[_]): Seq[RDD[_]] = {
    val roots = new ArrayBuffer[RDD[_]]()
    val visited = new ArrayBuffer[RDD[_]]()
    val stack = new mutable.Stack[RDD[_]]()
    stack.push(rdd)
    while (stack.nonEmpty) {
      val current = stack.pop()
      if (current.dependencies.nonEmpty) {
        var keepSearching = true
        for (parent <- current.dependencies if keepSearching) {
          if (!visited.contains(parent.rdd)) {
            stack.push(parent.rdd)
            keepSearching = false
          }
        }
      } else {
        roots += current
      }
      visited += current
    }
    roots
  }

  private def createPrefetchJob(rdd: RDD[_]): StreamPrefetchJob = {
    val hadoop = rdd.asInstanceOf[HadoopRDD[_, _]]
    val streamPartitions = hadoop.partitions.
      map(p => {
        val fileSplit = p.asInstanceOf[HadoopPartition].inputSplit.value.asInstanceOf[FileSplit]
        StreamPrefetchPartition(p, fileSplit.getPath, fileSplit.getStart, fileSplit.getLength)})
    val mapToLocations: Map[Partition, Seq[TaskLocation]] = rdd.partitions.map(
      partition => (partition, dag.getPreferredLocs(rdd, partition.index))
    ).toMap
    val plans = streamPartitions.map(sp => new StreamPrefetchPlan(
      RDDBlockId(rdd.id, sp.partition.index), sp, mapToLocations(sp.partition)))
    val entries = new mutable.HashMap[StreamPrefetchPlan, PrefetchTaskResult]()
    plans.foreach(plan => entries(plan) = null)
    new StreamPrefetchJob(rdd, entries)
  }

  private def makePrefetchOffers(): Seq[PrefetchOffer] = {
    // Under normal case we should check alive executors firstly.
    val exeData = cgsb_.retrieveExeDataForPrefetch
    while (exeData.isEmpty) {
      synchronized {
        wait(100)
      }
    }
    exeData.map {
      case (id, executorData) =>
        PrefetchOffer(id, executorData.executorHost)
    }.toSeq
  }

  private def makeSchedules(job: StreamPrefetchJob, offers: Seq[PrefetchOffer], cores: Int):
  Array[Array[StreamPrefetchDeployment]] = {
    val hostToExecutors = new mutable.HashMap[String, ArrayBuffer[String]]()
    for (o <- offers) {
      hostToExecutors.getOrElseUpdate(o.host, new ArrayBuffer[String]()) += o.executorId
    }
    val planScheduler = new StreamPrefetchPlanScheduler(offers,
      hostToExecutors, job.entries.keys.toSeq)
    planScheduler.makeResources(cores)
  }
}
