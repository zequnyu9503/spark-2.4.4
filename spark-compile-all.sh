#!/bin/bash
# the wohole project
mvn -DskipTests clean package
chmod 777 ./sbin/*
chmod 777 ./bin/*