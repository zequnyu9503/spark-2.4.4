#!/usr/bin/env bash
#host=$(hostname)
#if [[ $(hostname) = "centos3" ]];then
#sh halt.sh
#fi
#git pull
#mvn clean install -DskipTests -pl core,assembly,examples
sh halt.sh
slaves=("centos3" "centos4" "centos5" "centos11" "centos12" "centos13")
for slave in ${slaves[@]}
 do
  ssh "zc@${slave}" "cd /home/zc/service/spark-2.4.4/;git pull;mvn clean install -DskipTests -pl core,assembly,examples" &
 done
wait
sh start.sh