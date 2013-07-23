package com.grooveshark.hadoop.mappers;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;

import org.apache.hadoop.mapreduce.Counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

import com.grooveshark.hadoop.entities.CounterTrack;

public class Neo4jUsersMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>
{
    private String mapTaskId;
    private int noRecords = 0;
    private int batch = 0;
    private String dbPath = "";
    private JobConf jobConf;

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.jobConf = job;
        this.dbPath = job.get("neo4j_db_path");
        System.out.println("dbPath: " + this.dbPath);
    }

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
        throws IOException
    {
        Counter usersCounter = reporter.getCounter(CounterTrack.NEO4J_USERS_RECORD);
        long numUsers = usersCounter.getValue();
        output.collect(new LongWritable(++numUsers/50000), value);
        usersCounter.increment(1);
        this.noRecords++;
    }

    public void close()
    {
        System.out.println("Number of records from the mapper: " + this.noRecords);
    }

}
