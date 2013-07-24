package com.grooveshark.hadoop.reducers;

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
import java.util.Iterator;

import com.grooveshark.util.db.MysqlAccess;

public class GeonamesLoadReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, NullWritable>
{
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
            this.mysqlAccess = new MysqlAccess(
                    job.get("mysqlUrl"),
                    job.get("mysqlUser"),
                    job.get("mysqlPass"));
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException
    {
        ++this.noRecords;
        while(values.hasNext()) {
            String[] splits = values.next().toString().split("\t");
            String city = splits[1].trim();
            String asciiCity = split[2].trim();
            String alternateCities = split[3].trim();
            String latitude = splits[4].trim();
            String longitude = splits[5].trim();
            String countrycode = splits[8].trim();
            String region = splits[10].trim();
        }
    }

    public void close()
    {
        System.out.println("Num records processed: " + this.noRecords);
    }

}
