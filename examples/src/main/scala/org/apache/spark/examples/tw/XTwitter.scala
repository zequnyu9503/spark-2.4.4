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
import org.apache.spark.storage.StorageLevel

object XTwitter {

  val spliter = " "

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Twitter-" + System.currentTimeMillis())
    val sc = new SparkContext(conf)

    val common = new TwitterCommon(sc)

    val twitter = common.loadAsTwitter(args(0)).persist(StorageLevel.MEMORY_ONLY_SER)
    val langText = twitter.
      map(t => t.text.split(spliter).foreach(seg => LangText(t.lang, seg))).
      map(lt => (lt, 1L)).
      reduceByKey(_ + _).persist(StorageLevel.MEMORY_ONLY_SER)
    langText.count()
  }
}
