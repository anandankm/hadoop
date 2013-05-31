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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;


public class ExplodeUniqueReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, IntWritable>
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
        System.out.println("Outputfile: " + this.outputFile);
    }

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, IntWritable> output, Reporter reporter)
        throws IOException
    {
        HashSet<String> sids = new HashSet<String>();
        this.noRecords++;
        int size = 0;
        while (values.hasNext()) {
            sids.add(values.next().toString());
        }
        output.collect(key, new IntWritable(sids.size()));
    }

    public void close()
    {
        System.out.println("Number of records from the reducer: " + this.noRecords);
    }

}
