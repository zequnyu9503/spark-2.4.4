#!/usr/bin/env bash
sh halt.sh
git pull
mvn clean package -DskipTests -pl examples
sh start.sh