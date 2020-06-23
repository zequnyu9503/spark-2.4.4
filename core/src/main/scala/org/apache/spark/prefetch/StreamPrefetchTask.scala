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

import java.io.ByteArrayOutputStream
import java.net.URI
import java.nio.ByteBuffer

import scala.collection.mutable.ListBuffer

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{Partition, SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.storage.{BlockId, BlockManager, StorageLevel}
import org.apache.spark.util.io.ChunkedByteBuffer


class StreamLoader(conf: SparkConf, partition: StreamPrefetchPartition) {

  private var fs_ : FileSystem = initFS("zc")

  private val minMemoryMapBytes = conf.getSizeAsBytes("spark.storage.memoryMapThreshold", "2m")
  private val maxMemoryMapBytes = conf.get(config.MEMORY_MAP_LIMIT_FOR_TESTS)

  def generateChunkedByteBuffer: ChunkedByteBuffer = {
    var left = partition.len
    val chunks = new ListBuffer[ByteBuffer]()
    val ins = fs_.open(new Path(partition.path))
    val baos = new ByteArrayOutputStream()
    IOUtils.copyLarge(ins, baos, partition.offset, partition.len)
    val chunk = ByteBuffer.wrap(baos.toByteArray)
    fs_.close()
    chunks += chunk
    new ChunkedByteBuffer(chunks.toArray)
  }

  def initFS(user: String): FileSystem = {
    val path = "hdfs://centos3:9000"
    val conf = new Configuration()
    FileSystem.get(new URI(path), conf, user)
  }
}

case class StreamMeta(blockId: BlockId, level: StorageLevel,
                      partition: StreamPrefetchPartition) extends Serializable

case class StreamPrefetchPartition(partition: Partition, path: String,
                                   offset: Long, len: Long) extends Serializable

class StreamPrefetchTask(blockManager: BlockManager)
  extends Serializable with Logging{

  def startPrefetchTask(meta: StreamMeta): Option[(Long, Long)] = {
    val load_start = System.currentTimeMillis()
    val streamLoader = new StreamLoader(SparkEnv.get.conf, meta.partition)
    val chunkedByteBuffer = streamLoader.generateChunkedByteBuffer
    val load_end = System.currentTimeMillis()
    logInfo(s"Pefetch partition ${meta.blockId} costs ${load_end - load_start} ms.")
    if (blockManager.putBytes(meta.blockId, chunkedByteBuffer, meta.level)) {
      Option((load_end - load_start, System.currentTimeMillis() - load_end))
    } else {
      None
    }
  }
}
