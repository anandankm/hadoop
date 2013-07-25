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
hivePrefix="/user/hive/warehouse"
hiveTable="geonames_all_cities"
partitionColumns=""
partitionValues=""
jobClass="com.grooveshark.hadoop.jobs.GeonamesLoad"
jsonfile="mysql_export.json"
jsonElement="search"
echo "Running hadoop jar"

hadoop jar $jobJar com.grooveshark.hadoop.jobs.JobIssuer \
    -libjars $libjars \
    --jobJar $jobJar \
    --hivePrefix $hivePrefix \
    --hiveTable $hiveTable \
    --jobClass $jobClass \
    --myJson $jsonfile \
    --jsonElement $jsonElement \
