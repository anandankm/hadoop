#!/bin/bash
#mvn package || exit
## copy over gHadoop.jar
jobJar="lib/gHadoop.jar"
cp $jobJar ~/lib/

for i in /usr/lib/hive/lib/*.jar
do
   HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
done
libjars=""
for i in ~/lib/*.jar
do
   HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$i
   libjars="$i,$libjars"
done
libjars=$(echo $libjars | sed 's/,$//')

export HADOOP_CLASSPATH=$HADOOP_CLASSPATH
hivePrefix="/user/anandan.rangasamy"
hiveTable="ar_uniqs_daily_artists"
partitionColumns="uniq_type,year,datecomputed"
partitionValues="sessionid,2012:2013,2012-01-01:2013-06-11"
outputPath="/user/anandan.rangasamy/tmp/artists_uniqs_daily"
echo "Running hadoop jar"

hadoop jar $jobJar com.grooveshark.hadoop.jobs.ExplodeUnique \
    -libjars $libjars \
    --jobJar $jobJar \
    --partitionValues $partitionValues \
    --hivePrefix $hivePrefix \
    --hiveTable $hiveTable \
    --partitionColumns  $partitionColumns \
    --outputPath $outputPath
#sudo -uhdfs sh -c "
#export HADOOP_CLASSPATH=$HADOOP_CLASSPATH
#hadoop jar lib/gHadoop.jar com.grooveshark.hadoop.jobs.ExplodeUnique -libjars $libjars -myjson dsn.json -run Sequence
#"
