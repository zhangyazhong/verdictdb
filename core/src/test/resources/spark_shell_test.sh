#!/usr/bin/env bash

# transfer verdict.jar to master
scp /Users/zyz/IdeaProjects/verdictdb/core/target/verdict-core-0.4.7.jar hadoop@10.141.212.124:~/


# run spark with verdict.jar
~/spark-2.2.1-bin-hadoop2.7/bin/spark-shell --executor-memory 6g --driver-memory 2g --executor-cores 8 --jars ~/verdict-core-0.4.7.jar