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

import com.alibaba.fastjson.JSON
import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkContext}

object LocalResult {

  val root = "hdfs://centos3:9000/real-world/"

  def main(args: Array[String]): Unit = {
    val file = new File(args(0))
    val conf = new SparkConf().setAppName("RDDMemorySize-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)

    var record = ""
    for (i <- 1 to 5) {
      val origin = sc.textFile(s"$root/2019-4-${"%02d".format(i)}.json")
      origin.cache().count()
      val input = sc.rddCacheInMemory(origin)
      val result = origin.map(line => JSON.parseObject(line)).
        map(json => (i, json.getOrDefault("text", "").toString)).
        map(_._2).flatMap(txt => txt.split(" ")).
        map(e => (e, 1L)).reduceByKey(_ + _).cache()
      val start = System.currentTimeMillis()
      result.count()
      val end = System.currentTimeMillis()
      val local = sc.rddCacheInMemory(result)
      record += s"${end - start},$input,$local\n"

      origin.unpersist(true)
      result.unpersist(true)
    }

    Files.write(record, file, StandardCharsets.UTF_8)
  }
}
