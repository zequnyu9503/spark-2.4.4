#!/bin/bash
# Only core and assembly.
mvn -pl :spark-core_2.11 :spark-assembly_2.11 -DskipTests clean package
chmod 777 ./sbin/*
chmod 777 ./bin/*