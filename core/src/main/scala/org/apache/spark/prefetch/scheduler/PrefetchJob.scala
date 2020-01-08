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

import org.apache.spark.prefetch.{PrefetchReporter, SinglePrefetchTask}
import org.apache.spark.rdd.RDD

class PrefetchJob(val rdd: RDD[_],
                  val tasks: mutable.HashMap[SinglePrefetchTask[_], PrefetchReporter],
                  val callback: Seq[PrefetchReporter] => Unit = null) {

  def isAllFinished: Boolean = tasks.exists(_._2.eq(null))

  def count: Long = tasks.size

  def updateTaskStatusById(taskId: String, reporter: PrefetchReporter): Unit = {
    tasks.find(_._1.taskId.equals(taskId)) match {
      case Some(task) =>
        tasks(task._1) = reporter
      case _ =>
    }
  }
}
