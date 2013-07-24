package com.grooveshark.hadoop.mappers;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.LinkedList;

import com.grooveshark.util.db.MysqlAccess;

public class GeonamesLoadMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>
{
    private MysqlAccess mysqlAccess;
    private String mapTaskId;
    private String inputFile;
    private int noRecords = 0;
    private JobConf jobConf;

    public void configure(JobConf job)
    {
        System.out.println("Entering configure");
        this.mapTaskId = job.get("mapred.task.id");
        this.inputFile = job.get("map.input.file");
        System.out.println("inputfile: " + this.inputFile);
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
        throws IOException
    {
        ++this.noRecords;
        String[] splits = value.toString().split("\t");
        output.collect(new Text(splits[8]), value);
    }

    public void close()
    {
        System.out.println("Num records processed: " + this.noRecords);
    }

}
