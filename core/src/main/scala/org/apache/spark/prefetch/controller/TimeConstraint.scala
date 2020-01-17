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
package org.apache.spark.prefetch.controller

import scala.collection.mutable

class TimeConstraint {

  private val prefetch_ = new mutable.HashMap[String, (Long, Long)]()
  private val computation_ = new mutable.HashMap[String, (Long, Long)]()

  private def prefetchTime(): Long = {
    0L
  }

  private def waitingTime(): Long = {
    0L
  }

  protected def updatePrefetchTime(jobId: String, size: Long, duration: Long): Unit = {
    prefetch_(jobId) = (size, duration)
  }

  protected def updateComputationTime(jobId: String, size: Long, duration: Long): Unit = {
    computation_(jobId) = (size, duration)
  }

  protected def isAllowed: Boolean = waitingTime() >= prefetchTime()
}
