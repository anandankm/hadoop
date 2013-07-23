#!/bin/bash
mvn package || exit
## copy over gHadoop.jar
jobJar="target/gHadoop-1.0-jar-with-dependencies.jar"
cp $jobJar ~/lib/

for i in /usr/lib/hive/lib/*.jar
do
    HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done
libjars=""
for i in ~/lib/*.jar
do
    resolv_i=$(readlink $i)
    if [ -z "$resolv_i" ]
    then
        HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
        libjars="$i,$libjars"
    else
        HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$resolv_i
        libjars="$resolv_i,$libjars"
    fi
done
libjars=$(echo $libjars | sed 's/,$//')

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH
hivePrefix="/user/anandan.rangasamy"
hiveTable="ar_usersfollowers"
partitionColumns="tslogged"
partitionValues="1372737602"
echo "Running hadoop jar"

hadoop jar $jobJar com.grooveshark.hadoop.jobs.RestNeo4j \
    -libjars $libjars \
    --jobJar $jobJar \
    --partitionValues $partitionValues \
    --hivePrefix $hivePrefix \
    --hiveTable $hiveTable \
    --partitionColumns  $partitionColumns \
