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
package pers.yzq.timewindow

import java.io.{BufferedReader, InputStreamReader}

import scala.collection.immutable.HashMap

import com.alibaba.fastjson.JSON
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.{SparkConf, SparkContext}



object SentimentClassification {

  def dictionary: HashMap[String, (Float, Float)] = {
    val map = new HashMap[String, (Float, Float)]()
    val conf = new Configuration()
    conf.set("fs.default.name", "hdfs://node6:9000")
    val fs = FileSystem.get(conf)
    val path = new Path("/real-world/SentiWordNet.txt")
    val stream = new InputStreamReader(fs.open(path))
    val reader = new BufferedReader(stream)
    var line: String = reader.readLine()
    while(line != null) {
      if (!line.charAt(0).equals('#')) {
        val formats = line.split(" ")
        val PosScore = formats(2)
        val NegScore = formats(3)
        var word = formats(4)
        word = word.substring(0, word.indexOf("#"))
        map(word) = (PosScore.toFloat, NegScore.toFloat)
      }
      line = reader.readLine()
    }
    map
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sentiment Classification")
    val sc = new SparkContext(conf)

    val bc = sc.broadcast(dictionary)

    val raw = sc.textFile("hdfs://node6:9000/real-world/2019-4-01.json")
    val sentiment = raw.
      map(str => JSON.parseObject(str)).
      filter(!_.containsKey("delete")).
      filter(_.getString("text").length() > 0).
      filter(_.getBooleanValue("retweeted").equals(false)).
      filter(_.getString("lang").equals("en")).
      map(json => (json.getJSONObject("user").getString("id_str"), json)).
      groupByKey().
      map(user => {
        var sum = 0f
        var count: Long = 0
        val map = bc.value
        user._2.foreach(json => {
          val ts = json.getString("timestamp_ms")
          val content = json.getString("text").toLowerCase()
          val words = content.split(" ").filter(StringUtils.isAlphanumeric)
          words.foreach(word => {
            map.get(word) match {
              case Some(score) => sum = score._1 - score._2
              case _ =>
            }
            count += 1
          })
        })
        if (count > 0) sum /= count
        (user, sum)
      })
    sentiment.saveAsTextFile(s"/results/${System.currentTimeMillis()}")
  }
}
