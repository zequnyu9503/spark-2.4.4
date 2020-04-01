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

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.storage.{BlockId, RDDBlockId}

class SecondMigrationSuite extends SparkFunSuite{

  // scalastyle:off println

  private val cores = mutable.HashMap("0" -> 4, "1" -> 4, "2" -> 4, "3" -> 4, "4" -> 4)
  private val partitions = mutable.HashMap(
    RDDBlockId(0, 1) -> "0",
    RDDBlockId(0, 2) -> "0",
    RDDBlockId(0, 3) -> "0",
    RDDBlockId(0, 4) -> "1",
    RDDBlockId(0, 5) -> "1",
    RDDBlockId(0, 6) -> "2",
    RDDBlockId(0, 7) -> "2",
    RDDBlockId(0, 8) -> "2",
    RDDBlockId(0, 9) -> "2",
    RDDBlockId(0, 10) -> "3",
    RDDBlockId(0, 11) -> "3",
    RDDBlockId(0, 12) -> "3",
    RDDBlockId(0, 13) -> "4").collect{
    case (b, e) => (b.asInstanceOf[BlockId], e)
  }
  private val mem = mutable.HashMap(
    RDDBlockId(0, 1) -> 1000L,
    RDDBlockId(0, 2) -> 800L,
    RDDBlockId(0, 3) -> 1100L,
    RDDBlockId(0, 4) -> 600L,
    RDDBlockId(0, 5) -> 800L,
    RDDBlockId(0, 6) -> 600L,
    RDDBlockId(0, 7) -> 800L,
    RDDBlockId(0, 8) -> 1000L,
    RDDBlockId(0, 9) -> 900L,
    RDDBlockId(0, 10) -> 800L,
    RDDBlockId(0, 11) -> 600L,
    RDDBlockId(0, 12) -> 1100L,
    RDDBlockId(0, 13) -> 700L).collect {
    case (b, e) => (b.asInstanceOf[BlockId], e)
  }
  private val ser = 100L
  private val deser = 100L
  private val net = 300L
  private val next = 20L

  private val migration = new SecondMigration(cores, partitions, mem, ser, deser, net, next)

  test("k") {
    println(s"total memory ${mem.values.sum} total cores ${cores.values.sum}")
    println(migration.k())
  }

  test("ideal") {
    println(migration.ideal())
  }

  test("executors") {
    println(migration.executors)
  }

  test("isTriggered") {
    val workload = migration.executors.collect {
      case (executorId, blockIds) => (executorId, blockIds.map(id => mem(id)).sum)
    }
    println(workload)
    migration.setDist(2000L)
    println(migration.isTriggered(workload))
  }

  test("migrate") {
    migration.setDist(2000L)
    println(migration.migrate())
  }
}
