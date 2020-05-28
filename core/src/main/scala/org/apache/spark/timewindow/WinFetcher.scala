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
package org.apache.spark.timewindow

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.prefetch.cluster.{PrefetchBackend, PrefetchPlan}
import org.apache.spark.prefetch.scheduler.PrefetchScheduler

class WinFetcher[T, V] (sc: SparkContext,
                  controller: WindowController[T, V, _],
                  scheduler: PrefetchScheduler)
  extends Runnable with Logging {

  @volatile
  private var isRunning = true

  val backend = new PrefetchBackend(sc, scheduler)

  def updateWinId(id: Int): Unit = synchronized {
    backend.updateWinId(id)
  }

  // default waiting duration.
  private val waiting: Long = 1000

  def start(): Unit = synchronized {
    isRunning = true
  }

  def stop(): Unit = synchronized {
    isRunning = false
  }

  def continue(): Unit = synchronized {
    this.notifyAll()
  }

  def suspend(): Unit = synchronized {
    this.wait(waiting)
  }

  override def run(): Unit = {
    // scalastyle:off println
    synchronized {
      while (isRunning) {
        isAllowed(controller.winId.get()) match {
          case Some(plan) => doPrefetch(plan)
          case None =>
        }
        suspend()
      }
    }
  }

  private def isAllowed(id: Int): Option[PrefetchPlan[T, V]] = {
    if (id == 0) return None
    val plan = new PrefetchPlan[T, V](id, controller.randomWindow(id))
    Option(plan).filter(backend.canPrefetch)
  }

  private def doPrefetch(plan: PrefetchPlan[T, V]): Unit = {
    backend.doPrefetch(plan)
  }
}