#!/usr/bin/env bash
SPARK_HOME="/home/zc/service/spark-2.4.4"
if [[ ${host} = "centos3" ]];then
sh ${SPARK_HOME}/sbin/stop-master.sh
sh ${SPARK_HOME}/sbin/stop-slaves.sh
sh ${SPARK_HOME}/sbin/stop-history-server.sh
fi