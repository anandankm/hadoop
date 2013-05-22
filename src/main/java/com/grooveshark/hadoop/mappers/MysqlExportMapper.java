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
import com.grooveshark.util.db.MysqlWritable;

public class MysqlExportMapper extends MapReduceBase implements Mapper<LongWritable, MysqlWritable, NullWritable, Text>
{
    static enum MyCounter
    {
        NUM_RECORDS
    }
    private MysqlAccess mysqlAccess;
    private String mapTaskId;
    private String outputFile;
    private int noRecords = 0;
    private JobConf jobConf;
    private LinkedList<String> values = new LinkedList<String>();

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.outputFile = job.get("mapred.output.dir");
        this.jobConf = job;
        System.out.println("Outputfile: " + this.outputFile);
    }

    public void map(LongWritable key, MysqlWritable value, OutputCollector<NullWritable, Text> output, Reporter reporter)
        throws IOException
    {
        this.noRecords++;
        output.collect(NullWritable.get(), new Text(value.fields));
    }

    public void close()
    {
        System.out.println("Number of records from mapper: " + this.noRecords);
    }

}
