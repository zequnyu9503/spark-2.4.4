#!/usr/bin/env bash
libs_dir="hdfs://centos3:9000/libs"
log_path="/home/zc/service/spark-2.4.4/conf/log4j.properties"
spark-shell \
--master spark://centos3:7079 \
--executor-memory 16g \
--executor-cores 6 \
--driver-cores 6 \
--driver-memory 12g \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
--jars \
${libs_dir}/fastjson-1.2.35.jar \
