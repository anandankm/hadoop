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

public class MysqlLoadMapper extends MapReduceBase implements Mapper<LongWritable, Text, NullWritable, NullWritable>
{
    static enum MyCounter
    {
        NUM_RECORDS
    }
    private MysqlAccess mysqlAccess;
    private String mapTaskId;
    private String inputFile;
    private int noRecords = 0;
    private JobConf jobConf;
    private LinkedList<String> values = new LinkedList<String>();

    public void configure(JobConf job)
    {
        System.out.println("Entering configure");
        this.mapTaskId = job.get("mapred.task.id");
        this.inputFile = job.get("map.input.file");
        this.jobConf = job;
        try {
            this.mysqlAccess = new MysqlAccess();
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void map(LongWritable key, Text value, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException
    {
        ++this.noRecords;
        reporter.incrCounter(MyCounter.NUM_RECORDS, 1l);
        if (this.noRecords%1000 == 0) {
            String reportStr = mapTaskId + " processed " + this.noRecords + " from input-file: " + this.inputFile;
            reporter.setStatus(reportStr);
        }
        this.values.add(value.toString());
        if (this.values.size() == 10000) {
            try {
                this.mysqlAccess.insertViaLoad(this.values, this.jobConf.get("mysql.table"));
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            this.values.clear();
        }
    }

    public void close()
    {
        System.out.println("Num records processed: " + this.noRecords);
        if (this.values.size() > 0) {
            try {
                this.mysqlAccess.insertViaLoad(this.values, this.jobConf.get("mysql.table"));
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            System.out.println("Last left size: " + this.values.size());
        }
    }

}
