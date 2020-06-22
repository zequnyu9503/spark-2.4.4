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
package org.apache.spark.prefetch.scheduler

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.prefetch.{PrefetchOffer, StreamPrefetchDeployment, StreamPrefetchPlan}
import org.apache.spark.scheduler.TaskLocality

class StreamPrefetchPlanScheduler(offers: Seq[PrefetchOffer],
                                  hostToExecutors: mutable.HashMap[String, ArrayBuffer[String]],
                                  plans: Seq[StreamPrefetchPlan]) {

  private val forHosts =
    new mutable.HashMap[String, ArrayBuffer[StreamPrefetchPlan]]()
  private val forAll = new ArrayBuffer[StreamPrefetchPlan]()

  def isAllScheduled: Boolean = forAll.isEmpty

  addPendingTasks()

  private def addPendingTasks(): Unit = {
    for (i <- plans.indices) {
      for (loc <- plans(i).locs) {
        forHosts.getOrElseUpdate(loc.host, new ArrayBuffer[StreamPrefetchPlan]()) += plans(i)
      }
      forAll += plans(i)
    }
  }

  def removeTask(taskId: String): Unit = {
    for (host <- forHosts.keys) {
      forHosts(host).find(_.taskId.equals(taskId)) match {
        case Some(toBeRem) => forHosts(host) -= toBeRem
        case None =>
      }
    }
    forAll.find(_.taskId.equals(taskId)) match {
      case Some(any) => forAll -= any
      case None =>
    }
  }

  private def pickTaskFromOffer(offer: PrefetchOffer): Option[StreamPrefetchDeployment] = {
    if (forHosts.nonEmpty && forHosts.contains(offer.host)) {
      val onHost = forHosts(offer.host)
      if (onHost.nonEmpty) {
        val plan = forHosts(offer.host)(0)
        val deploy = new StreamPrefetchDeployment(offer.executorId,
          plan.taskId, plan.toMeta, TaskLocality.NODE_LOCAL)
        return Option(deploy)
      }
    }
    if (forAll.nonEmpty) {
      val plan = forAll(0)
      val deploy = new StreamPrefetchDeployment(offer.executorId,
        plan.taskId, plan.toMeta, TaskLocality.ANY)
      return Option(deploy)
    }
    None
  }

  protected[prefetch] def makeResources(cores_exe: Int): Array[Array[StreamPrefetchDeployment]] = {
    val descriptions = new ArrayBuffer[Array[StreamPrefetchDeployment]]()
    var subDesc = new ArrayBuffer[StreamPrefetchDeployment]()
    while (!isAllScheduled) {
      var cores = 0
      while (cores < cores_exe) {
        for (offer <- offers) {
          pickTaskFromOffer(offer) match {
            case Some(desc) =>
              subDesc += desc
              removeTask(desc.taskId)
            case None =>
          }
        }
        cores += 1
      }
      descriptions += subDesc.toArray
      subDesc = new ArrayBuffer[StreamPrefetchDeployment]()
      cores = 0
    }
    descriptions.toArray
  }
}
