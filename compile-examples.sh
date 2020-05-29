#!/usr/bin/env bash
sh halt.sh
git pull
mvn clean install -DskipTests -pl examples