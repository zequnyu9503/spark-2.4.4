#!/usr/bin/env bash
libs_dir="hdfs://centos3:9000/libs"
log_path="/home/zc/service/spark-2.4.4/conf/log4j.properties"
rm /home/zc/service/spark-2.4.4/logs/*
spark-submit \
--master spark://centos3:7079 \
--executor-memory 16g \
--executor-cores 6 \
--driver-cores 6 \
--driver-memory 16g \
--class org.apache.spark.examples.tw.Velocity \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
examples/target/original-spark-examples_2.11-2.4.4.jar \