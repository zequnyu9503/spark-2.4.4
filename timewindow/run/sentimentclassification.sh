#!/usr/bin/env bash
libs_dir="hdfs://node6:9000/libs"
log_path="/opt/service/spark/spark-2.4.4/conf/log4j.properties"
spark-submit \
--master spark://node6:7079 \
--executor-memory 32g \
--executor-cores 32 \
--driver-cores 16 \
--driver-memory 16g \
--class pers.yzq.timewindow.SentimentClassification \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
--jars ${libs_dir}/fastjson-1.2.35.jar
