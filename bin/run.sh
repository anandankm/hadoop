#!/bin/bash
WORKING_DIR=$(dirname $(dirname $(readlink -f $0)));
cd $WORKING_DIR
ant build
hadoop jar lib/hadoop-job.jar com.grooveshark.hadoop.jobs.MysqlLoad $WORKING_DIR
