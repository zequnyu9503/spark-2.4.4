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

import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkContext}

object Velocity {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Velocity").
      set("cores.prefetch.executors", "8")
    val sc = new SparkContext(conf)

    val scheduler = sc.prefetchScheduler
    val file = new File("/home/zc/yzq/repoters.txt")

    val rdd_0 = sc.textFile("hdfs://centos3:9000/real-world/2019-4-02.json")
    val rdd_1 = sc.textFile("hdfs://centos3:9000/real-world/2019-4-01.json")

    rdd_1.count()
    rdd_1.count()

    new Thread(new Runnable {
      override def run(): Unit = {
        scheduler.prefetch(rdd_0) match {
          case Some(reporters) =>
            reporters.foreach(reporter => {
              if (reporter != null) {
                Files.append(reporter.toString + "\n", file, StandardCharsets.UTF_8)
              } else {
                Files.append(s"Fail ${reporter.taskId}" + "\n", file, StandardCharsets.UTF_8)
              }
            })
          case _ =>
        }

      }
    }).start()

    rdd_1.count()
    rdd_1.count()
    rdd_1.count()
    rdd_1.count()
    rdd_1.count()
    rdd_1.count()
    rdd_1.count()

  }
}
