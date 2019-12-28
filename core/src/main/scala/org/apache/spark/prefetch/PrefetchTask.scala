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
package org.apache.spark.prefetch

import java.nio.ByteBuffer

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class PrefetchTask[T](taskBinary: Broadcast[Array[Byte]], partition: Partition)
    extends Serializable {

  private var paused: Boolean = true

  def startTask(context: TaskContext): Unit = {
    val ser = SparkEnv.get.closureSerializer.newInstance()
    val rdd = ser.deserialize[RDD[T]](
      ByteBuffer.wrap(taskBinary.value),
      Thread.currentThread.getContextClassLoader)
    val iterator = rdd.iterator(partition, context)
    while (iterator.hasNext) {iterator.next()}
  }
}
