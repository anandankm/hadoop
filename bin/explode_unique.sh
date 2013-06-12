#!/bin/bash
mvn package || exit
## copy over gHadoop.jar 
cp lib/gHadoop.jar ~/lib/

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
hadoop jar lib/gHadoop.jar com.grooveshark.hadoop.jobs.ExplodeUnique -libjars $libjars --myjson dsn.json.copy --partitionValues sessionid,2012:2013,2012-01-01:2013-06-09
#sudo -uhdfs sh -c "
#export HADOOP_CLASSPATH=$HADOOP_CLASSPATH
#hadoop jar lib/gHadoop.jar com.grooveshark.hadoop.jobs.ExplodeUnique -libjars $libjars -myjson dsn.json -run Sequence
#"
