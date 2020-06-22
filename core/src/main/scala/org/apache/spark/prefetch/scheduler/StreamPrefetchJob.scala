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

import org.apache.spark.prefetch.{PrefetchTaskResult, StreamPrefetchPlan}
import org.apache.spark.rdd.RDD

class StreamPrefetchJob(val rdd: RDD[_],
                        val entries: mutable.HashMap[StreamPrefetchPlan, PrefetchTaskResult]) {

  def isAllFinished: Boolean = !entries.exists(_._2.eq(null))

  def taskCount: Long = entries.size

  def updateTaskById(taskId: String, result: PrefetchTaskResult): Unit = {
    entries.find(_._1.taskId.equals(taskId)) match {
      case Some(task) =>
        entries(task._1) = result
      case _ =>
    }
  }

  def retrieveResults(): Seq[PrefetchTaskResult] = entries.values.toSeq
}
