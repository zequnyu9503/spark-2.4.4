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
package org.apache.spark.examples

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.prefetch.PrefetchReporter

object PrefetchTest {

  def main(args: Array[String]): Unit = {
    val input = args(0)
    val prefetched = args(1)

    val conf = new SparkConf().setAppName("PrefetchTest-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)

    val rdd_0 = sc.textFile(input)
    val rdd_1 = sc.textFile(prefetched)

    // scalastyle:off println

    // Make sure that rdd_1 cached in memory.
    sc.prefetchRDD(rdd_1, (reporters: Seq[PrefetchReporter]) => {
      System.err.println(s"Longest prefetch duration is ${reporters.maxBy(_.duration)}")
      System.err.println(s"Largest prefetch size is ${reporters.maxBy(_.elements)}")
      System.err.println("RDD_1 has" + rdd_1.count() + "elements.")
    })
    System.err.println("RDD_0 has" + rdd_0.count() + "elements.")
  }
}
