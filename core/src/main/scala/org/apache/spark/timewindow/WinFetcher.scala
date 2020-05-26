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
import org.apache.spark.rdd.RDD


class WinFetcher (sc: SparkContext,
                  controller: WindowController[_, _],
                  scheduler: PrefetchScheduler)
  extends Runnable with Logging {

  @volatile
  private var isRunning = false

  private val backend = new PrefetchBackend(sc, scheduler)

  private val maxPrefetchStep = 3

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
        for (plus <- 1 to maxPrefetchStep) {
          isAllowed(controller.winId.get() + plus) match {
            case Some(rdd) => doPrefetch(rdd)
            case None =>
          }
        }
        suspend()
      }
    }
  }

  private def isAllowed(id: Int): Option[RDD[_]] = {
    val plan = new PrefetchPlan(id, controller.randomWindow(id))
    if (backend.canPrefetch(plan)) Option(plan.rdd) else None
  }

  private def doPrefetch[T](rdd: RDD[T]): Unit = {
    val thr = new Thread(new Runnable {
      override def run(): Unit = {
        scheduler.prefetch(rdd)
      }
    })
    thr.start()
  }
}

object WinFetcher {

  var winFetcher: WinFetcher = _

  def service(sparkContext: SparkContext,
              controller: WindowController[_, _],
              scheduler: PrefetchScheduler): WinFetcher = {
    if (winFetcher eq null) {
      winFetcher = new WinFetcher(sparkContext, controller, scheduler)
      val thr = new Thread(winFetcher)
      winFetcher.start()
      thr.start()
    }
    winFetcher
  }
}
