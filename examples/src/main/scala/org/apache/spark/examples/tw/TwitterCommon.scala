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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class TwitterCommon(sc: SparkContext) {

  def langs: Array[String] =
    Array(
      "am",
      "ja",
      "ru",
      "ur",
      "ar",
      "pa",
      "dv",
      "ml",
      "sd",
      "gu",
      "pl",
      "si",
      "mr",
      "sl",
      "vi",
      "bg",
      "ps",
      "pt",
      "my",
      "sr",
      "ckb",
      "ka",
      "bn",
      "sv",
      "el",
      "hi",
      "en",
      "ne",
      "es",
      "et",
      "eu",
      "km",
      "ta",
      "kn",
      "ko",
      "nl",
      "ht",
      "hu",
      "te",
      "no",
      "ca",
      "th",
      "hy",
      "fa",
      "tl",
      "zh",
      "tr",
      "fi",
      "und",
      "cs",
      "in",
      "fr",
      "cy",
      "is",
      "lo",
      "it",
      "iw",
      "da",
      "lt",
      "lv",
      "or",
      "ro",
      "uk",
      "de"
    )

  def load(file: String): RDD[String] = sc.textFile(file)

  def loadAsTwitter(file: String): RDD[TwitterData] =
    load(file)
      .map(line => JSON.parseObject(line))
      .filter(json => !json.containsKey("delete"))
      .map(json =>
        TwitterData(
          json.getLong("id"),
          json.getOrDefault("text", "").toString,
          json.getJSONObject("user").getLong("id"),
          json.getJSONObject("user").getString("name"),
          json.getJSONObject("user").getString("description"),
          json.getJSONObject("user").getString("created_at"),
          json.getOrDefault("lang", "default").toString,
          json.getString("timestamp_ms").toLong
      ))
}
