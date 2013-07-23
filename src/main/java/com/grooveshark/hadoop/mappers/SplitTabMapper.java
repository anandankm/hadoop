package com.grooveshark.hadoop.mappers;

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

import java.io.IOException;
import java.util.LinkedList;


public class SplitTabMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>
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

    public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
        throws IOException
    {
        String[] values = value.toString().split("\t");
        if (values.length <= 1) {
            return;
        }
        values[0] = values[0].trim();
        values[1] = values[1].trim();
        if (values[0].contains("NULL") || values[0].contains("\\N") || values[0].isEmpty()
                || values[1].contains("NULL") || values[1].contains("\\N") || values[1].isEmpty()
           ) {
            return;
        }
        output.collect(new LongWritable(Long.parseLong(values[0])), new Text(values[1]));
        this.noRecords++;
    }

    public void close()
    {
        System.out.println("Number of records from the mapper: " + this.noRecords);
    }

}
