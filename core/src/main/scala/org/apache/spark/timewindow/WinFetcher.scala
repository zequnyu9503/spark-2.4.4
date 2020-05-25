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
import org.apache.spark.prefetch.cluster.PrefetchBackend
import org.apache.spark.prefetch.scheduler.PrefetchScheduler
import org.apache.spark.rdd.RDD


class WinFetcher (sc: SparkContext, scheduler: PrefetchScheduler)
  extends Runnable with Logging {

  @volatile
  private var isRunning = false

  // Current running window id.
  @volatile
  private var winId: Int = 0

  private val backend = new PrefetchBackend(sc, scheduler)

  def updateWinId(id: Int): Unit = synchronized {
    winId = id
    backend.updateWinId(winId)
  }

  // default waiting duration.
  private var waiting: Long = 1000

  def stop(): Unit = synchronized {
    isRunning = false
  }

  def continue(): Unit = synchronized {
    this.notifyAll()
  }

  def suspend(): Unit = synchronized {
    this.wait(waiting)
  }

  {
    logInfo("Win Fetcher service is ready.")
    synchronized {isRunning = true}
  }

  override def run(): Unit = {
    // scalastyle:off println
    synchronized {
      while (isRunning) {
        if (isAllowed) {
          backend.canPrefetch(null)
        }
        suspend()
      }
    }
  }

  private def isAllowed: Boolean = {
    false
  }

  private def doPrefetch[T](rdd: RDD[T]): Unit = {
    val thr = new Thread(new Runnable {
      override def run(): Unit = {

      }
    })
    thr.start()
  }
}

object WinFetcher {

  var winFetcher: WinFetcher = _

  def service(sparkContext: SparkContext, scheduler: PrefetchScheduler): WinFetcher = {
    if (winFetcher eq null) {
      winFetcher = new WinFetcher(sparkContext, scheduler)
      val thr = new Thread(winFetcher)
      thr.start()
    }
    winFetcher
  }
}
