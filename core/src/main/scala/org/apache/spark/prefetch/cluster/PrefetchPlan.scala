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
package org.apache.spark.prefetch.cluster

import org.apache.spark.prefetch.PrefetchTaskDescription
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocality.TaskLocality


class PrefetchPlan(winId$: Int, rdd$: RDD[_]) {
  var schedule: Array[Array[PrefetchTaskDescription]] = _

  def maxLocality: Array[TaskLocality] = {
    if (!schedule.eq(null)) {
      schedule.map(batch => batch.maxBy(_.locality).locality)
    } else null
  }

  def partitions: Int = rdd$.partitions.length

  def winId: Int = winId$

  def rdd[T, V]: RDD[(T, V)] = rdd$.asInstanceOf[RDD[(T, V)]]
}
