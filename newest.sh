#!/usr/bin/env bash
slaves=("centos3" "centos4" "centos5" "centos11" "centos12" "centos13")
for slave in ${slaves[@]}
 do
  ssh "zc@${slave}" "cd /home/zc/service/spark-2.4.4/;git pull" &
 done
wait