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

case class TwitterData(
                 var id: Long,
                 var text: String,
                 var userId: Long,
                 var userName: String,
                 var description: String,
                 var userCreated: String,
                 var lang: String,
                 var ts: Long) {
}

case class LangText(lang: String, text: String) {

  override def hashCode(): Int = {
    lang.hashCode * 31 + text.hashCode * 31
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case lt: LangText =>
        lt.lang.equals(lang) && lt.text.equals(text)
      case _ => false
    }
  }
}