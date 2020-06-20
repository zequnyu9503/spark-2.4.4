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

class TUser(
                   var id: Long,
                   var id_str: String,
                   var name: String,
                   var screen_name: String,
                   var location: String,
                   var description: String,
                   var followers_count: Long,
                   var friends_count: Long,
                   var listed_count: Long,
                   var favourites_count: Long,
                   var statuses_count: Long,
                   var created_at: String,
                   var lang: String,
                   var profile_background_color: String,
                   var profile_background_image_url: String,
                   var profile_background_image_url_https: String,
                   var profile_background_tile: Boolean,
                   var profile_link_color: String,
                   var profile_sidebar_border_color: String,
                   var profile_sidebar_fill_color: String,
                   var profile_text_color: String,
                   var profile_use_background_image: Boolean,
                   var profile_image_url: String,
                   var profile_image_url_https: String,
                   var profile_banner_url: String,
                   var default_profile: Boolean,
                   var default_profile_image: Boolean) {

  override def equals(obj: Any): Boolean = {
    obj match {
      case user: TUser => user.id.equals(id)
      case _ => false
    }
  }

  override def hashCode(): Int = {
    id_str.hashCode * 31
  }

}

object TUser {

  def assemble(user: JSONObject): TUser = {
    if (user.eq(null)) return null
    new TUser(
      user.getLong("id"),
      user.getString("id_str"),
      user.getString("name"),
      user.getString("screen_name"),
      user.getString("location"),
      user.getString("description"),
      user.getLong("followers_count"),
      user.getLong("friends_count"),
      user.getLong("listed_count"),
      user.getLong("favourites_count"),
      user.getLong("statuses_count"),
      user.getString("created_at"),
      user.getString("lang"),
      user.getString("profile_background_color"),
      user.getString("profile_background_image_url"),
      user.getString("profile_background_image_url_https"),
      user.getBoolean("profile_background_tile"),
      user.getString("profile_link_color"),
      user.getString("profile_sidebar_border_color"),
      user.getString("profile_sidebar_fill_color"),
      user.getString("profile_text_color"),
      user.getBoolean("profile_use_background_image"),
      user.getString("profile_image_url"),
      user.getString("profile_image_url_https"),
      user.getString("profile_banner_url"),
      user.getBoolean("default_profile"),
      user.getBoolean("default_profile_image")
    )
  }
}
