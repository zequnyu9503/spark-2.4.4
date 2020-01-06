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

import java.io.{DataInputStream, DataOutputStream}
import java.nio.ByteBuffer

import org.apache.spark.util.{ByteBufferInputStream, ByteBufferOutputStream, Utils}

class PrefetchTaskDescription(val executorId: String, val taskId: String,
                              val serializedTask: ByteBuffer) {
}

object PrefetchTaskDescription {
  def encode(taskDescription: PrefetchTaskDescription): ByteBuffer = {
    val bytesOut = new ByteBufferOutputStream(4096)
    val dataOut = new DataOutputStream(bytesOut)

    dataOut.writeUTF(taskDescription.executorId)
    dataOut.writeUTF(taskDescription.taskId)
    Utils.writeByteBuffer(taskDescription.serializedTask, bytesOut)

    dataOut.close()
    bytesOut.close()
    bytesOut.toByteBuffer
  }

  def decode(byteBuffer: ByteBuffer): PrefetchTaskDescription = {
    val dataIn = new DataInputStream(new ByteBufferInputStream(byteBuffer))
    val executorId = dataIn.readUTF()
    val taskId = dataIn.readUTF()
    val serializedTask = byteBuffer.slice()
    new PrefetchTaskDescription(executorId, taskId, serializedTask)
  }
}
