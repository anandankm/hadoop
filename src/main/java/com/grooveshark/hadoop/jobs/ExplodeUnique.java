package com.grooveshark.hadoop.jobs;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import com.google.gson.JsonElement;

import com.grooveshark.util.FileUtils;
import com.grooveshark.util.StringUtils;
import com.grooveshark.util.DateUtils;
import com.grooveshark.util.db.DBAccess;
import com.grooveshark.util.db.DBProperties;
import com.grooveshark.util.db.MysqlAccess;
import com.grooveshark.util.db.MysqlWritable;
import com.grooveshark.hadoop.mappers.ExplodeUniqueMapper;
import com.grooveshark.hadoop.mappers.ExplodeUniqueReducer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.FileAppender;

public class ExplodeUnique extends Configured implements Tool
{
    public static Logger log = Logger.getLogger("MysqlExport");
    public String jobJar;
    public String hivePrefix;
    public String hiveTable;
    public String partitionColumns;
    public String partitionValues;
    public String inputPaths = "";
    public String outputPath = "";
    public String threadName;
    public String[] args;

    public void setup(String jsonFile) {
        try {
            this.threadName = Thread.currentThread().getName();
            String[] explode = {"explode"};
            JsonElement je = FileUtils.parseJson(jsonFile, explode);
            this.jobJar = FileUtils.getJsonValue(je, "jobJar");
            this.hivePrefix = FileUtils.getJsonValue(je, "hivePrefix");
            this.hiveTable = FileUtils.getJsonValue(je, "hiveTable");
            this.partitionColumns = FileUtils.getJsonValue(je, "partitionColumns");
            this.partitionValues = FileUtils.getJsonValue(je, "partitionValues");
            this.outputPath = FileUtils.getJsonValue(je, "outputPath");
            StringUtils.logToStdOut(this.threadName, this.jobJar);
            StringUtils.logToStdOut(this.threadName, this.hivePrefix);
            StringUtils.logToStdOut(this.threadName, this.hiveTable);
            StringUtils.logToStdOut(this.threadName, this.partitionColumns);
            StringUtils.logToStdOut(this.threadName, this.partitionValues);
            StringUtils.logToStdOut(this.threadName, this.outputPath);
            this.createInputPaths();
            StringUtils.logToStdOut(this.threadName, this.inputPaths);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void createInputPaths() {
        String[] pColumns = this.partitionColumns.split(",");
        String[] pValues = this.partitionValues.split(",");
        String path = this.hivePrefix + this.hiveTable + "/";
        for (int i=0; i < pColumns.length; i++) {
            String pCol = pColumns[i];
            pCol = pCol.trim();
            String pVal = pValues[i].trim();
            path += pCol + "=" + pVal + "/";
        }
        this.inputPaths += path.substring(0, path.length() - 1);
    }

    public int run(String[] args) throws IOException {
        StringUtils.logToStdOut(this.threadName, "Starting the Explode Unique job.");
        JobConf conf = (JobConf) this.getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
        conf.setJobName("ExplodeUnique: " + this.partitionValues);
        conf.setMapperClass(ExplodeUniqueMapper.class);
        conf.setReducerClass(ExplodeUniqueReducer.class);
        conf.setNumReduceTasks(5);
        conf.setNumMapTasks(5);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        try {
            Path outPath = new Path(this.outputPath);
            if (FileUtils.isHDFSFileExists(outPath, conf)) {
                StringUtils.logToStdOut(this.threadName, "output file path: " + this.outputPath + " exists.");
                if (!FileUtils.deleteHDFSFile(outPath, conf)) {
                    StringUtils.logToStdOut(this.threadName, "output file path: " + this.outputPath + " cannot be deleted. Abort!");
                    System.exit(1);
                }
                StringUtils.logToStdOut(this.threadName, "Deleted: " + this.outputPath);
            }
            FileInputFormat.setInputPaths(conf, this.inputPaths);
            FileOutputFormat.setOutputPath(conf, outPath);
            conf.setJar(this.jobJar);
            StringUtils.logToStdOut(this.threadName, "Starting hadoop job");
            long start = System.currentTimeMillis();
            JobClient.runJob(conf);
            float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
            StringUtils.logToStdOut(this.threadName, "Done ("+elapsed+" secs).");
        } catch (Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
        return 0;
    }

    public static void main(String[] args) {
        InputStream is = FileUtils.getInputStream("log4j.properties");
        if (is != null) {
            StringUtils.logToStdOut("main", "Setting log4j properties");
            PropertyConfigurator.configure(is);
        }
        String jsonFile = "";
        if (args.length > 0) {
            for (int i = 0; i<args.length; i++) {
                if (args[i].equals("-myjson")) {
                    jsonFile = args[i+1];
                    break;
                }
            }
        } else {
            StringUtils.logToStdOut("main", "Please provide mysql/hadoop/hive parameters using a json file");
            StringUtils.logToStdOut("main", "json file should be provided as -myjson <jsonfile>");
            StringUtils.logToStdOut("main", "Refer run.sh and sample.dsn.json");
            System.exit(1);
        }
        try {
            ExplodeUnique explodeUnique = new ExplodeUnique();
            StringUtils.logToStdOut("main", "JsonFile: " + jsonFile);
            explodeUnique.setup(jsonFile);
            JobConf conf = new JobConf();
            ToolRunner.run(conf, explodeUnique, args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
