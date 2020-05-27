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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class TimeWindowSuite extends SparkFunSuite {
  // scalastyle:off println

  test("time window api") {
    val conf = new SparkConf().
      set("cores.prefetch.executors", "4").
      setAppName("TimeWindow" + System.currentTimeMillis()).
      setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.textFile("E:\\Project\\Scala Project\\experiments\\app-21-30.txt")
    rdd.cache().count()


    val itr = new TimeWindowRDD[Long, Long](sc, 10, 10, (start: Long, end: Long) => {
      sc.textFile(s"E:\\Project\\Scala Project\\experiments\\app-$start-$end.txt").
        map(e => e.split(" ")).
        map(e => (e(0).toLong, e(1).toLong))
    }).setScope(1, 31).iterator()


    while(itr.hasNext) {
      val rdd = itr.next()
      println(s"Count is ${rdd.count()}")
    }
  }

  test("number format") {
    val a = 1
    println("%02d".format(a))

    val b = 11
    println("%02d".format(b))
  }

  test ("json") {
    val conf = new SparkConf().
      set("cores.prefetch.executors", "4").
      setAppName("json" + System.currentTimeMillis()).
      setMaster("local")
    val sc = new SparkContext(conf)
  }
}
