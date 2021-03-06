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


public class ExplodeUniqueMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>
{
    private String mapTaskId;
    private String outputFile;
    private int noRecords = 0;
    private JobConf jobConf;
    private Text sid = new Text();
    private LinkedList<String> values = new LinkedList<String>();

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
        this.noRecords++;
        String[] values = value.toString().split("\t");
        if (values[0].contains("NULL") || values[0].contains("\\N")) {
            return;
        }
        values[1] = values[1].trim();
        if (values[1].isEmpty()) {
            return;
        }
        LongWritable aid = new LongWritable(Long.parseLong(values[0]));
        String[] sesArr = values[1].split(",");
        for (String ses : sesArr) {
            this.sid.set(ses);
            output.collect(aid, this.sid);
        }
    }

    public void close()
    {
        System.out.println("Number of records from the mapper: " + this.noRecords);
    }

}
