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
package org.apache.spark.timewindow

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class TimeWindowSuite extends SparkFunSuite {
  // scalastyle:off println

  test("Time Window") {
    val conf = new SparkConf().setMaster("local").setAppName("TimeWindow")
    val sc = new SparkContext(conf)

    val iterator = new TimeWindowRDD[Long, Long](sc, 10, 10, (start: Long, end: Long) => {
      val seq = new ArrayBuffer[(Long, Long)]()
      for (t <- start until end) {
        seq.+=((t, Math.random().toLong))
      }
      sc.parallelize(seq)
    }).iterator()

    while (iterator.hasNext) {
      val timeWindowRDD = iterator.next()
      println(timeWindowRDD.count())
    }
  }

  test("thread") {
    val conf = new SparkConf().setMaster("local").setAppName("thread")
    val sc = new SparkContext(conf)
    println("start fetcher")
    val winFetcher = WinFetcher.service(sc)
    val rdd = sc.textFile("F:\\Resources\\txt.txt")
    println(rdd.count())
  }
}
