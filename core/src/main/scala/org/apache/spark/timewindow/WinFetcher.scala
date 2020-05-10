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

import scala.concurrent._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}

import ExecutionContext.Implicits.global

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ThreadUtils


class WinFetcher (sc: SparkContext) extends Runnable with Logging {

  @volatile
  private var isRunning = false

  private var id: Int = 0

  // default waiting duration.
  private val waiting: Long = 1000

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
    synchronized {isRunning = true}
    logInfo("Win Fetcher service is ready.")
  }

  override def run(): Unit = {
    // scalastyle:off println
    synchronized {
      while (isRunning) {
        if (!isAllowed()) {
          logInfo("wait")
          suspend()
        } else {
          logInfo("do")
        }
        logInfo("run")
      }
    }
  }

  private def isAllowed(): Boolean = {
    false
  }

  private def doPrefetch[T](rdd: RDD[T]): Unit = {
    val future = Future[Unit] {
      println("yuzequn1")
    }
    ThreadUtils.awaitResult(future, Duration.Inf)

    future onComplete  {
      case Success(res) => println("ok")
      case Failure(fa) => println("fail")
    }
  }
}

object WinFetcher {

  var winFetcher: WinFetcher = _

  def service(sparkContext: SparkContext): WinFetcher = {
    if (winFetcher eq null) {
      winFetcher = new WinFetcher(sparkContext)
      val thr = new Thread(winFetcher)
      thr.start()
    }
    winFetcher
  }
}