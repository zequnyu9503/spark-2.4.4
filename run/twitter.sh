#!/usr/bin/env bash
log_path="/home/zc/service/spark-2.4.4/conf/log4j.properties"
spark-submit \
--master spark://centos3:7079 \
--executor-memory 12g \
--executor-cores 4 \
--driver-cores 4 \
--driver-memory 12g \
--class org.apache.spark.examples.tw.Twitter \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
--jars run/libs/fastjson-1.2.35.jar \
examples/target/original-spark-examples_2.11-2.4.4.jar
