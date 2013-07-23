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

import org.apache.hadoop.hive.serde2.MetadataTypedColumnsetSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;

import java.io.IOException;
import java.util.LinkedList;


public class ExplodeUniqueMapperSequence extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable, Text>
{
    static enum MyCounter
    {
        NUM_RECORDS
    }
    private String mapTaskId;
    private String outputFile;
    private int noRecords = 0;
    private JobConf jobConf;
    private Text sid = new Text();
    private MetadataTypedColumnsetSerDe serde;
    private LinkedList<String> values = new LinkedList<String>();

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.outputFile = job.get("mapred.output.dir");
        this.jobConf = job;
        System.out.println("Outputfile: " + this.outputFile);
    }

    public void map(LongWritable key, Text value, OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException
    {
        this.noRecords++;
        if (this.noRecords == 2) {
            String row = "";
            try {
                this.serde = new MetadataTypedColumnsetSerDe();
                row = (String) this.serde.deserialize(value);
            } catch (SerDeException e) {
                throw new IOException("Cannot instantiate MetadataTypedColumnsetSerDe", e);
            }
            System.out.println(row);
        }
        output.collect(NullWritable.get(), value);
    }

    public void close()
    {
        System.out.println("Number of records from the mapper: " + this.noRecords);
    }

}
