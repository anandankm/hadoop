package com.grooveshark.hadoop.reducers;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Iterator;

public class SplitTabReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, LongWritable>
{
    private String mapTaskId;
    private String outputFile;
    private int noRecords = 0;
    private JobConf jobConf;

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.outputFile = job.get("mapred.output.dir");
        this.jobConf = job;
        System.out.println("output dir: " + this.outputFile);
    }

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, LongWritable> output, Reporter reporter)
        throws IOException
    {
        this.noRecords++;
        while (values.hasNext()) {
            LongWritable writableLong = new LongWritable(Long.parseLong(values.next().toString()));
            output.collect(key, writableLong);
        }
    }

    public void close()
    {
        System.out.println("Number of records from the reducer: " + this.noRecords);
    }

}
