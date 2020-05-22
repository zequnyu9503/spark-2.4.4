#!/usr/bin/env bash
host=$(hostname)
if [[ ${hostname} = "centos3" ]];then
sh ~/yzq/sh/spark-halt.sh
fi
git pull
mvn clean install -DskipTests -pl core,assembly