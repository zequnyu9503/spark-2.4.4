#!/usr/bin/env bash
libs_dir="hdfs://centos3:9000/libs"
log_path="/home/zc/service/spark-2.4.4/conf/log4j.properties"
rm /home/zc/service/spark-2.4.4/logs/*
spark-submit \
--master spark://centos3:7079 \
--executor-memory 16g \
--executor-cores 4 \
--driver-cores 4 \
--driver-memory 16g \
--class org.apache.spark.examples.tw.HTwitter \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
--jars \
${libs_dir}/hbase-common-2.1.4.jar,\
${libs_dir}/hbase-client-2.1.4.jar,\
${libs_dir}/hbase-mapreduce-2.1.4.jar,\
${libs_dir}/hbase-protocol-2.1.4.jar,\
${libs_dir}/hbase-protocol-shaded-2.1.4.jar,\
${libs_dir}/hbase-shaded-miscellaneous-2.1.0.jar,\
${libs_dir}/hbase-shaded-netty-2.1.0.jar,\
${libs_dir}/hbase-shaded-protobuf-2.1.0.jar,\
${libs_dir}/htrace-core-3.1.0-incubating.jar,\
${libs_dir}/htrace-core4-4.2.0-incubating.jar,\
${libs_dir}/hbase-hadoop-compat-2.1.4.jar,\
${libs_dir}/hbase-hadoop2-compat-2.1.4.jar,\
${libs_dir}/hbase-metrics-2.1.4.jar,\
${libs_dir}/hbase-metrics-api-2.1.4.jar,\
${libs_dir}/hbase-server-2.1.4.jar,\
${libs_dir}/hbase-zookeeper-2.1.4.jar,\
${libs_dir}/fastjson-1.2.35.jar \
examples/target/original-spark-examples_2.11-2.4.4.jar \
Twitter-2019 April First