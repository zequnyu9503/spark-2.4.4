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

import com.alibaba.fastjson.JSONObject

class TwitterData(
                   var id: Long,
                   var id_str: String,
                   var text: Array[String],
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
    text = text.map(word => word.trim.
      replaceAll("[\\~\\!\\@\\#\\$\\%\\^\\&\\*\\(\\)\\<\\>\\[\\]\\-\\=\\+\\.]", "").
      replaceAll("[0-9]+", ""))
    this
  }

}

object TwitterData {
  def assemble(data: JSONObject): TwitterData = {
    new TwitterData(
      data.getLong("id"),
      data.getString("id_str"),
      data.getOrDefault("text", "").toString.split(" "),
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
}
