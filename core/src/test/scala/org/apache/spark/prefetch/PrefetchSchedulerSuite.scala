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

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.prefetch.cluster.PrefetchBackend

class PrefetchSchedulerSuite extends SparkFunSuite {

  // scalastyle:off println

  val conf = new SparkConf().setAppName("PrefetchSchedulerSuite-"
    + System.currentTimeMillis()).setMaster("local")
    .set("cores.prefetch.executors", "12")
    .set("expansion.hdfs", "1.833274997")
    .set("calc.prefetch", "1.50925e-6")
    .set("load.local.prefetch", "4.066275746634837E-5")
    .set("load.remote.prefetch", "0")
    .set("variation.prefetch", "0.018134686")
    .set("min.prefetch", "2")

  val sc = new SparkContext(conf)

  test("Forecast datasize of reality") {
    val daySize = Array(11825783079L, 9351172561L, 11243611348L, 20406366705L,
      19392728397L, 19682386615L, 21082494394L, 13313777046L, 19920937770L,
      20207386576L, 20086139197L, 21112343537L, 20102665431L, 20479471790L,
      19854391546L, 20017970190L, 20185603932L, 20264545173L, 19693594884L,
      17713865605L, 20320328049L, 21683713659L, 21337064106L, 21493198522L,
      21112255362L, 20402706128L, 20130745918L, 21136173886L, 20433117880L,
      21758667067L).map(tv => java.lang.Long.valueOf(tv))

    for (ds <- 2 to 29) {
      val subDaySize = new Array[java.lang.Long](ds)
      for (si <- subDaySize.indices) subDaySize(si) = daySize(si)
      val next = new DataSizeForecast().forecastNext(subDaySize)
      println(s"${next.toLong}")
    }
  }
}
