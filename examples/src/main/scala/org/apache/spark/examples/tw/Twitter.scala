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

import scala.util.parsing.json.JSON

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.timewindow.TimeWindowRDD

object Twitter extends Serializable {

  def main(args: Array[String]): Unit = {
    val root = "hdfs://centos3:9000/real-world/"
    val output = s"hdfs://centos3:9000/results/twitter-${System.currentTimeMillis()}"
    val winSize = 1
    val winStep = 1

    val conf = new SparkConf().setAppName("Twitter-" + System.currentTimeMillis())
      .set("cores.prefetch.executors", "4")
    val sc = new SparkContext(conf)

    def load(start: Long, end: Long): RDD[(Long, String)] = {
      sc.textFile(s"$root/2019-4-${"%02d".format(start)}.json").
        map(e => JSON.parseFull(e)).map {
        case Some(map: Map[String, Any]) =>
          (start, map.get("text").toString)
        case _ => (start, "")
      }
    }

    val itr = new TimeWindowRDD[Long, String](sc, 1, 1, load).
      setScope(1, 5).iterator()

    var local = sc.emptyRDD[(String, Long)]

    while (itr.hasNext) {
      val winRDD = itr.next()
      val result = winRDD.map(_._2).
        flatMap(_.split(" ")).
        map(e => (e, 1L)).reduceByKey(_ + _)
      local = local.union(result).cache()
      local.count()
    }
    local.reduceByKey(_ + _).saveAsTextFile(output)
  }
}
