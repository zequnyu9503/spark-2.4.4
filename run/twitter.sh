#!/usr/bin/env bash
libs_dir="hdfs://centos3:9000/libs"
log_path="/home/zc/service/spark-2.4.4/conf/log4j.properties"
rm /home/zc/service/spark-2.4.4/logs/*
spark-submit \
--master spark://centos3:7079 \
--executor-memory 16g \
--executor-cores 1 \
--driver-cores 4 \
--driver-memory 16g \
--class org.apache.spark.examples.tw.Twitter \
--driver-java-options "-Dlog4j.configuration=file:${log_path}" \
--jars ${libs_dir}/fastjson-1.2.35.jar,\
${libs_dir}/kuromoji-0.7.7.jar \
examples/target/original-spark-examples_2.11-2.4.4.jar \
20 \
25 \
false \
4 \
3 \
0.00009335577487945556640625 \
1.12326E-05
# 参数:
#窗口起始编号
#窗口结束编号
#是否预取
#预取并行度
#最大等待预取窗口数
#本地数据载入速度
#窗口计算速度
