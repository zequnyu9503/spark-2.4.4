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

  private val daySize = Seq(11825783079L, 9351172561L, 11243611348L, 20406366705L,
    19392728397L, 19682386615L, 21082494394L, 13313777046L, 19920937770L,
    20207386576L, 20086139197L, 21112343537L, 20102665431L, 20479471790L,
    19854391546L, 20017970190L, 20185603932L, 20264545173L, 19693594884L,
    17713865605L, 20320328049L, 21683713659L, 21337064106L, 21493198522L,
    21112255362L, 20402706128L, 20130745918L, 21136173886L, 20433117880L,
    21758667067L)

  def main(args: Array[String]): Unit = {
    val isPrefetch = args(0).toBoolean
    val start = args(1).toInt
    val end = args(2).toInt

    val root = "hdfs://centos3:9000/real-world/"
    val output = s"hdfs://centos3:9000/results/twitter-${System.currentTimeMillis()}"

    val conf = new SparkConf().setAppName("Twitter-" + System.currentTimeMillis())
      .set("cores.prefetch.executors", "8")
      .set("expansion.hdfs", "1.833274997")
      .set("calc.prefetch", "1.50925e-6")
      .set("load.local.prefetch", "4.27967E-06")
      .set("load.remote.prefetch", "0")
      .set("variation.prefetch", "0.018134686")
      .set("min.prefetch", "5")

    val sc = new SparkContext(conf)

    def load(start: Long, end: Long): RDD[(Long, String)] = {
      sc.textFile(s"$root/2019-4-${"%02d".format(start)}.json").
        map(line => JSON.parseObject(line)).
        map(json => (start, json.getOrDefault("text", "").toString))
    }

    val twRDD = new TimeWindowRDD[Long, String, (String, Long)](sc, 1, 1, load).
      setScope(start, end).
      setPartitionsLimitations(120).
      setStorageLevel(StorageLevel.MEMORY_ONLY).
      allowPrefetch(isPrefetch).
      setDaySize(daySize)
    val itr = twRDD.iterator()

    while (itr.hasNext) {
      val winRDD = itr.next()
      val result = winRDD.map(_._2).flatMap(txt => txt.split(" ")).
        map(e => (e, 1L)).reduceByKey(_ + _)
      result.persist(StorageLevel.MEMORY_AND_DISK).count()
      twRDD.saveLocalResult(result)
    }

    twRDD.localAsRDD().reduceByKey(_ + _).saveAsTextFile(output)
  }
}
