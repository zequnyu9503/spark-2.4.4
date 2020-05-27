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
package org.apache.spark.examples.tw

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkContext}


object RDDMemorySize {

  def main(args: Array[String]): Unit = {
    val file = new File(args(0))

    val conf = new SparkConf().setAppName("RDDMemorySize-" + System.currentTimeMillis())
      .set("cores.prefetch.executors", "8")
    val sc = new SparkContext(conf)

    var record: String = ""
    for (day <- 1 to 30) {
      val rdd = sc.textFile(s"hdfs://centos3:9000/real-world/2019-4-${"%02d".format(day)}.json")
      rdd.cache().count()
      val memorySize = sc.rddCacheInMemory(rdd)
      record += s"${"%02d".format(day)}>>$memorySize\n"
      rdd.unpersist(true)
    }
    Files.write(record, file, StandardCharsets.UTF_8)
  }
}
