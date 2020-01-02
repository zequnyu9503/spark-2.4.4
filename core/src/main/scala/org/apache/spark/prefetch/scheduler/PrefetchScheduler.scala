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

import scala.collection.Map

import org.apache.spark.{Partition, SparkContext, SparkEnv}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.prefetch.PrefetchTask
import org.apache.spark.prefetch.master.PrefetcherMaster
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{DAGScheduler, SchedulerBackend, TaskLocation, TaskScheduler}

class PrefetchScheduler(val sc: SparkContext,
                        val backend: SchedulerBackend,
                        val ts: TaskScheduler,
                        val dag: DAGScheduler,
                        val master: PrefetcherMaster)
    extends Logging {

  private val closureSerializer = SparkEnv.get.closureSerializer.newInstance()

  // core function.
  def prefetch(rdd: RDD[_]): Unit = {
    var taskBinary: Broadcast[Array[Byte]] = null
    val partitions: Array[Partition] = rdd.partitions
    var taskBinaryBytes: Array[Byte] = null

    try {
      taskBinaryBytes =
        JavaUtils.bufferToArray(closureSerializer.serialize(rdd: AnyRef))
    } catch {
      case e: NotSerializableException => return
    }
    taskBinary = sc.broadcast(taskBinaryBytes)
    val taskIdToLocations: Map[Partition, Seq[TaskLocation]] = partitions
      .map(
        partition => {
          (partition, dag.getPreferredLocs(rdd, partition.index))
        }
      )
      .toMap
    val pTasks: Seq[PrefetchTask[_]] = partitions.map(partition =>
      new PrefetchTask(taskBinary, partition, taskIdToLocations(partition))
    ).toSeq
    if (pTasks.nonEmpty) {
      logInfo(s"@YZQ Accept ${pTasks.size} prefetch tasks.")
      val taskManager = new PrefetchTaskManager(master, ts, backend, pTasks)

    } else {
      logError("@YZQ Reject prefetch tasks.")
    }
  }
}
