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

  private val variation = Seq(0.033356021, 0.031769108, 0.031319463, 0.031874393, 0.03140867,
    0.031733687, 0.032430361, 0.032185735, 0.03157043, 0.03277209, 0.032386915)

  def main(args: Array[String]): Unit = {
    val start = args(0).toInt
    val end = args(1).toInt
    val isPrefetch = args(2).toBoolean
    val prefetch_cores = args(3)
    val min = args(4)
    val load_local = args(5)
    val calc = args(6)

    val root = "hdfs://centos3:9000/real-world/"
    val output = s"hdfs://centos3:9000/results/twitter-${System.currentTimeMillis()}"

    val appName = s"Twitter-$start-$end-${if (isPrefetch) s"P-$prefetch_cores" else "NP"}"
    val conf = new SparkConf().setAppName(appName)
      .set("cores.prefetch.executors", prefetch_cores)
      .set("calc.velocity", calc)
      .set("load.local.prefetch", load_local)
      .set("load.remote.prefetch", "0")
      .set("min.prefetch", min)

    val sc = new SparkContext(conf)

    def load(start: Long, end: Long): RDD[(Long, String)] = {
      sc.textFile(s"$root/2019-4-${"%02d".format(start)}.json").
        map(line => JSON.parseObject(line)).
        filter(!_.eq(null)).
        filter(!_.containsKey("delete")).
        map(json => TwitterData.assemble(json)).
        map(_.washText()).
        flatMap(_.getWords).
        map(record => (start, record))
    }

    val twRDD = new TimeWindowRDD[Long, String, (String, Long)](sc, 1, 1, load).
      setScope(start, end).
      setPartitionsLimitations(120).
      setStorageLevel(StorageLevel.MEMORY_ONLY).
      allowPrefetch(isPrefetch).
      setDaySize(daySize).
      setExpansion(expansion).
      setVariation(variation)
    val itr = twRDD.iterator()

    while (itr.hasNext) {
      val winRDD = itr.next()
      val result = winRDD.map(tt => (tt._2, 1L)).reduceByKey(_ + _)
      result.persist(StorageLevel.MEMORY_ONLY_SER).count()
      twRDD.saveLocalResult(result)
    }

    twRDD.localAsRDD().reduceByKey(_ + _).saveAsTextFile(output)
  }
}
