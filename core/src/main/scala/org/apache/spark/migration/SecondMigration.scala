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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.storage.BlockId

class SecondMigration(
                      // executor, executorId
                      val cores: mutable.HashMap[String, Int],
                      // blockId, executorId
                      val partitions: mutable.HashMap[BlockId, String],
                      // blockId, cached size
                      val mem: mutable.HashMap[BlockId, Long],
                      val ser: Long,
                      val deSer: Long,
                      val net: Long,
                      val next: Long) {

  // Maximum manhattan distance of the cluster.
  private [migration] var MAX_DIST: Long = 1 * 1024

  // Ideal workload per CPU core.
  private [migration] val k_ = k()

  // Ideal workload per executor.
  private [migration] val ideal_ = ideal()

  // Here we clone partitions as blocks for migration.
  private [migration] val blocks = partitions.clone()

  // Mappings from executors to blocks. One-to-many.
  private [migration] val executors = mutable.HashMap[String, List[BlockId]](blocks.
    groupBy(_._2).collect { case (exeId, bIds) =>
    (exeId, bIds.keys.toList)}.toArray: _*)

  private [migration] def setDist(dist: Long): Unit = MAX_DIST = dist

  // Calculate manhattan distance by two memory hash maps.
  // The shorter hash map will be calculated.
  private [migration] def calcManhattan(
                                        hm1: mutable.HashMap[String, Long],
                                        hm2: mutable.HashMap[String, Long]): Long =
    hm1.collect { case (k, v) if hm2.contains(k) => (k, math.abs(v - hm2(k)))}.values.sum

  private [migration] def ideal(): mutable.HashMap[String, Long] = {
    cores.collect {case (k, v) => (k, v * k_)}
  }

  private [migration] def k(): Long = mem.values.sum / cores.values.sum

  private [migration] def isTriggered(hm: mutable.HashMap[String, Long]): Boolean = {
    // Manhattan distance beyond the cluster is a necessary condition for migration.
    // So for simplicity we choose cluster's distance as a trigger condition of migration.
    calcManhattan(hm, ideal_) > MAX_DIST
  }

  private [migration] def >|(hm: mutable.HashMap[String, Long]): mutable.HashMap[String, Long] =
    hm.collect {case (k, v) if v > ideal_(k) => (k, v)}

  private [migration] def |<(hm: mutable.HashMap[String, Long]): mutable.HashMap[String, Long] =
    hm.collect {case (k, v) if v < ideal_(k) => (k, v)}

  private [migration] def duration(size: Long): Long = size / (ser + deSer + net)

  private [migration] def pickMaxMax(hm: mutable.HashMap[String, Long],
                                  migrated: ArrayBuffer[MigrationPlan]):
  Option[(String, BlockId, Long)] = {
    var found = false
    // Executor, BlockId, Size of block.
    var one: (String, BlockId, Long) = null
    // Record executors and blocks for time limitation.
    val blackExecutors = new ArrayBuffer[String]()
    val blackBlocks = new ArrayBuffer[BlockId]()

    while (!found) {
      val whiteExecutors = hm.collect {
        case (exeId, size) if !blackExecutors.contains(exeId) => (exeId, size)
      }
      if (whiteExecutors.isEmpty) {
        return None
      } else {
        // Select the highest distance executors from whitelist.
        val maxExeSet = whiteExecutors.collect {
          case (exe, size) =>
            // Manhattan distance of each executor under various ability.
            val rest = math.abs(size - ideal_(exe))
            // Standardize computational ability of each executor.
            (exe, rest / cores(exe))
        }.groupBy(_._2).maxBy(_._1)._2.keys

        // It's better to filter out executors which were already migrated before.
        // Otherwise we choose the highest distance arbitrarily.
        val betterExeSet = Option(maxExeSet.
          filter(exe => !migrated.map(_.sourceId).contains(exe))) match {
          case Some(exeSet) => exeSet
          case _ => maxExeSet
        }
        for (exe <- betterExeSet if !found) {
          // Get blocks in decreasing order.
          val blocks = executors(exe).map(b => (b, mem(b))).sortBy(_._2).reverse
          for (block <- blocks if !found) {
            val missing = Option(migrated.filter(m => m.sourceId.equals(exe) &&
              m.blockId.equals(block._1)).map(_.blockId)) match {
              case Some(mbs) => duration(mbs.map(b => mem(b)).sum)
              case _ => 0L
            }
            if (duration(block._2) + missing <= next) {
              one = new Tuple3[String, BlockId, Long](exe, block._1, block._2)
              found = true
            } else {
              blackBlocks += block._1
            }
          }
          if (!found) blackExecutors += exe
        }
      }
    }
    Option(one)
  }

  private [migration] def pickMin(hm: mutable.HashMap[String, Long],
                                  migrated: ArrayBuffer[MigrationPlan]): Option[String] = {
    // Select the lowest distance executors from whitelist.
    val minExeSet = hm.collect {
      case (exe, size) =>
        val rest = math.abs(size - ideal_(exe))
        (exe, rest / cores(exe))
    }.groupBy(_._2).maxBy(_._1)._2.keys
    Option(minExeSet.
      filter(exe => !migrated.map(_.destinationId).contains(exe))) match {
      case Some(exeSet) => Option(exeSet.head)
      case _ => Option(minExeSet.head)
    }
  }

  private [migration] def migrate(): ArrayBuffer[MigrationPlan] = {
    // Memory used of all partitions per executor.
    val workload = executors.collect {
      case (executorId, blockIds) => (executorId, blockIds.map(id => mem(id)).sum)
    }

    // Used for recording migrations.
    val plans: ArrayBuffer[MigrationPlan] = new ArrayBuffer[MigrationPlan]()

    // Whether get enough time left for migration
    var keepGoing = true

    while (keepGoing && isTriggered(workload)) {
      val high = this.>|(workload)
      val low = this.|<(workload)

      // It's impossible to divide the workload into empty parts.
      if (high.isEmpty || low.isEmpty) return plans

      val max = pickMaxMax(high, plans) match {
        case Some(m) => m
        case _ => null
      }
      val min = pickMin(low, plans) match {
        case Some(m) => m
        case _ => null
      }

      if (max != null && min != null) {
        val source = max._1
        val blockId = max._2
        val destination = min

        // Record migration.
        plans += MigrationPlan(isLocal = false, isMem = true, source, blockId, destination)

        // Update blocks.
        blocks(blockId) = destination

        // Update workload.
        workload(source) -= mem(blockId)
        workload(destination) += mem(blockId)
      } else {
        keepGoing = false
      }
    }
    plans
  }

}
