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
package org.apache.spark.migration

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.BlockId

case class MigrationPlan(isLocal: Boolean,
                         isMem: Boolean,
                         sourceId: String,
                         blockId: BlockId,
                         destinationId: String) {
  override def toString: String = {
    val stringBuffer = new ArrayBuffer[String]()
    if (isLocal) {
      stringBuffer += "[Local Mode] "
      stringBuffer += "<" + sourceId + ">"
      stringBuffer += "{" + blockId + "}"
    }
    if (isMem) {
      stringBuffer += "[Remote Mode]"
      stringBuffer += "<" + sourceId + "-" + destinationId + ">"
      stringBuffer += "<" + blockId + ">"
    }
    stringBuffer.mkString(" ")
  }
}
