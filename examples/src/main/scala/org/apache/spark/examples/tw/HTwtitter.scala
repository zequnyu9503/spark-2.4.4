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

import com.alibaba.fastjson.JSON
import org.apache.hadoop.hbase.{Cell, CompareOperator, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.{BinaryComparator, FamilyFilter, FilterList, QualifierFilter}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object HTwtitter extends Serializable {

  def main(args: Array[String]): Unit = {
    val tableName = args(0)
    val columnFamily = args(1)
    val columnQualify = args(2)
    val start = args(3).toLong
    val end = args(4).toLong

    val output = s"hdfs://centos3:9000/results/twitter-${System.currentTimeMillis()}"

    val conf = new SparkConf().setAppName("Twitter-" + System.currentTimeMillis())
      .set("cores.prefetch.executors", "4")
      .set("expansion.hdfs", "1.833274997")
      .set("calc.prefetch", "1.50925e-6")
      .set("load.local.prefetch", "3.912299871444702e-5")
      .set("load.remote.prefetch", "0")
      .set("variation.prefetch", "0.018134686")
      .set("min.prefetch", "1")

    val sc = new SparkContext(conf)

    def load(startstamp: Long, endstamp: Long): RDD[(Long, String)] = {
      loadRDD(sc, tableName, columnFamily, columnQualify, startstamp, endstamp).
        map(result => {
          val cell: Cell = result._2.listCells().get(0)
          val line = JSON.parseObject(Bytes.toString(cell.getValueArray))
          (cell.getTimestamp, line.getOrDefault("text", "").toString)
        })
    }

    def loadRDD(sc: SparkContext, tableName: String, columnFamily: String,
                columnQualify: String, start: Long, end: Long)
    : RDD[(ImmutableBytesWritable, Result)] = {
      val hc = HBaseConfiguration.create()
      val hBaseConfiguration = HBaseConfiguration.create()
      hBaseConfiguration.set("hbase.zookeeper.quorum",
        "centos3,centos4,centos5,centos11,centos12,centos13")
      hBaseConfiguration.set("hbase.master", "centos3:6000")
      hc.set(TableInputFormat.INPUT_TABLE, tableName)
      hc.set(TableInputFormat.SCAN,
        TableMapReduceUtil.convertScanToString(new Scan()
            .setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,
              new FamilyFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes(columnFamily))),
              new QualifierFilter(CompareOperator.EQUAL,
                new BinaryComparator(Bytes.toBytes(columnQualify)))
            )).setTimeRange(start, end)))
      sc.newAPIHadoopRDD(hc, classOf[TableInputFormat],
        classOf[ImmutableBytesWritable], classOf[Result])
    }

    val winRDD = load(start, end)
    val result = winRDD.map(_._2).flatMap(txt => txt.split(" ")).
            map(e => (e, 1L)).reduceByKey(_ + _)
    result.saveAsTextFile(output)

//    val twRDD = new TimeWindowRDD[Long, String, (String, Long)](sc, 1, 1, load).
//      setScope(1, 5).allowPrefetch(true)
//    val itr = twRDD.iterator()
//
//    while (itr.hasNext) {
//      val winRDD = itr.next()
//      val result = winRDD.map(_._2).flatMap(txt => txt.split(" ")).
//        map(e => (e, 1L)).reduceByKey(_ + _)
//      result.cache().count()
//      twRDD.saveLocalResult(result)
//    }
//
//    twRDD.localAsRDD().reduceByKey(_ + _).saveAsTextFile(output)
  }
}
