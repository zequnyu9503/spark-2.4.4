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

import com.alibaba.fastjson.JSON
import org.slf4j.LoggerFactory

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.timewindow.TimeWindowRDD

object Twitter extends Serializable {

  private val logger = LoggerFactory.getLogger("tw")

  private val daySize = Seq(6498962854L, 6922091286L, 7330817391L,
    7208953523L, 7247935818L, 7119822237L, 6859754598L, 6819414955L,
    7129508195L, 6899126870L, 7342710769L)

  private val expansion = Seq(1.905530046, 1.874726205, 1.919632459, 1.891447636, 1.90908936,
  1.917948468, 1.903823376, 1.925796942, 1.869058846, 1.882587519, 1.916559424)

  def main(args: Array[String]): Unit = {
    val isPrefetch = args(0).toBoolean
    val start = args(2).toInt
    val end = args(3).toInt
    val prefetch_cores = args(1).toInt

    val root = "hdfs://centos3:9000/real-world/"
    val output = s"hdfs://centos3:9000/results/twitter-${System.currentTimeMillis()}"

    val appName = s"Twitter-$start-$end-${if (isPrefetch) s"P-$prefetch_cores" else "NP"}"
    val conf = new SparkConf().setAppName(appName)
      .set("cores.prefetch.executors", prefetch_cores.toString)
      .set("expansion.hdfs", "1.833274997")
      .set("calc.prefetch", "1.50925e-6")
      .set("load.local.prefetch", "4.27967E-06")
      .set("load.remote.prefetch", "0")
      .set("variation.prefetch", "0.018134686")
      .set("min.prefetch", "3")

    val sc = new SparkContext(conf)

    def load(start: Long, end: Long): RDD[(Long, String)] = {
      sc.textFile(s"$root/2019-4-${"%02d".format(start)}.json").
        map(line => JSON.parseObject(line)).
        filter(!_.containsKey("delete")).
        map(json => TwitterData.assemble(json)).
        filter(_.text.length > 0).
        map(_.washText()).
        flatMap(_.text).
        map(_.toLowerCase).
        map(word => (start, word))
    }

    val twRDD = new TimeWindowRDD[Long, String, (String, Long)](sc, 1, 1, load).
      setScope(start, end).
      setPartitionsLimitations(120).
      setStorageLevel(StorageLevel.MEMORY_ONLY).
      allowPrefetch(isPrefetch).
      setDaySize(daySize).
      setExpansion(expansion)
    val itr = twRDD.iterator()

    while (itr.hasNext) {
      val winRDD = itr.next()
      val result = winRDD.map(tt => (tt._2, 1L)).reduceByKey(_ + _)
      result.persist(StorageLevel.MEMORY_AND_DISK).count()
      twRDD.saveLocalResult(result)
    }

    twRDD.localAsRDD().reduceByKey(_ + _).saveAsTextFile(output)
  }
}
