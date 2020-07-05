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

import scala.collection.JavaConverters._

import com.alibaba.fastjson.JSONObject
import org.atilika.kuromoji.Tokenizer

class TwitterData(
                   var id: Long,
                   var id_str: String,
                   var text: String,
                   var source: String,
                   var truncated: Boolean,
                   var favorited: Boolean,
                   var retweeted: Boolean,
                   var user: TUser,
                   var filter_level: String,
                   var lang: String,
                   var ts: Long) {

  override def equals(obj: Any): Boolean = {
    obj match {
      case data: TwitterData =>
        data.id.equals(id)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    id_str.hashCode * 31
  }

  def washText(): TwitterData = {
    // Filter marks.
    text = text.trim.
      replaceAll("[\\~\\!\\@\\#\\$\\%\\^\\&\\*\\(\\)\\<\\>\\[\\]\\-\\=\\+\\.]", " ")
    this
  }

  def getWords: Array[String] = {
    if (text == null || text.length == 0) return Array()
    lang match {
      case "en" => TwitterData.breakEn(text)
      case "ja" => TwitterData.breakJap(text)
      case _ => TwitterData.breakAny(text)
    }
  }

}

object TwitterData {

  private val tokenizer = Tokenizer.builder().build()

  def assemble(data: JSONObject): TwitterData = {
    new TwitterData(
      data.getLong("id"),
      data.getString("id_str"),
      data.getOrDefault("text", "").toString,
      data.getString("source"),
      data.getBoolean("truncated"),
      data.getBoolean("favorited"),
      data.getBoolean("retweeted"),
      TUser.assemble(data.getJSONObject("user")),
      data.getString("filter_level"),
      data.getString("lang"),
      data.getString("timestamp_ms").toLong
    )
  }

  def breakJap(origin: String): Array[String] = {
    val token = tokenizer.tokenize(origin).asScala
    token.map(_.getSurfaceForm).toArray
  }

  def breakEn(origin: String): Array[String] = {
    origin.split(" ").filter(_.length > 0)
  }

  def breakAny(origin: String): Array[String] = {
    origin.split(" ").filter(_.length > 0)
  }
}
