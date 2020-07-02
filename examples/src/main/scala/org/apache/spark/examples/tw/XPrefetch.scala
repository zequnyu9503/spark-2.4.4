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

import org.apache.spark.{SparkConf, SparkContext}

object XPrefetch {


  def main(args: Array[String]): Unit = {
    val appName = "XPrefetch"
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)

    val toBePrefetched = sc.textFile("hdfs://centos3:9000/real-word/2019-04-30.json")

    sc.textFile("hdfs://centos3:9000/real-word/2019-04-20.json").count()
    new Thread(new Runnable {
      override def run(): Unit = {
        sc.prefetch(toBePrefetched, 2)
      }
    }).start()
    sc.textFile("hdfs://centos3:9000/real-word/2019-04-20.json").count()
    sc.textFile("hdfs://centos3:9000/real-word/2019-04-20.json").count()
    toBePrefetched.count()
  }
}
