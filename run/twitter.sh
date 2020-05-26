#!/usr/bin/env bash
spark-submit \
--master spark://centos3:7079 \
--executor-memory 12g \
--executor-cores 4 \
--driver-cores 4 \
--driver-memory 12g \
--class org.apache.spark.examples.tw.Twitter \
examples/target/original-spark-examples_2.11-2.4.4.jar
