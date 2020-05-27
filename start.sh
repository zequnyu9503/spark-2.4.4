#!/usr/bin/env bash
MASTER_URL="spark://centos3:7079"
SPARK_HOME="/home/zc/service/spark-2.4.4"
slaves=("centos4" "centos5" "centos11" "centos12" "centos13")
if [[ $(hostname) = "centos3" ]];then
echo "start..."
sh "${SPARK_HOME}/sbin/start-master.sh"
for slave in ${slaves[@]}
 do
  ssh "zc@${slave}" sh "${SPARK_HOME}/sbin/start-slave.sh" -h ${slave} ${MASTER_URL}
 done
sh "${SPARK_HOME}/sbin/start-history-server.sh"
fi

