#!/usr/bin/env bash
spark-shell \
--master spark://centos3:7079 \
--executor-memory 12g \
--executor-cores 4 \
--driver-cores 4 \
--driver-memory 8g