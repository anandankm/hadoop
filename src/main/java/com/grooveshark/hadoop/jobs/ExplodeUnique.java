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
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import com.google.gson.JsonElement;

import com.grooveshark.util.FileUtils;
import com.grooveshark.util.StringUtils;
import com.grooveshark.util.DateUtils;
import com.grooveshark.util.db.DBAccess;
import com.grooveshark.util.db.DBProperties;
import com.grooveshark.util.db.HiveProperties;
import com.grooveshark.util.db.MysqlAccess;
import com.grooveshark.util.db.MysqlWritable;
import com.grooveshark.hadoop.mappers.ExplodeUniqueMapper;
import com.grooveshark.hadoop.mappers.ExplodeUniqueMapperSequence;
import com.grooveshark.hadoop.reducers.ExplodeUniqueReducer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.FileAppender;

public class ExplodeUnique extends Configured implements Tool
{
    public static Logger log = Logger.getLogger(ExplodeUnique.class);
    public String jobJar;
    public String jsonFile;
    public String hivePrefix;
    public String hiveTable;
    public String hivePropertiesFile;
    public LinkedList<String> partitionColumns;
    public LinkedList<String> partitionValues;
    public String inputPaths = "";
    public String outputPath = "";
    public String threadName;
    public String runCmd = "";
    public String[] args;
    public HiveProperties hiveProperties = null;

    public void setup() {
        try {
            this.threadName = Thread.currentThread().getName();
            if (this.jobJar == null && this.jsonFile != null) {
                String[] explode = {"explode"};
                JsonElement je = FileUtils.parseJson(this.jsonFile, explode);
                this.jobJar = FileUtils.getJsonValue(je, "jobJar");
            }
            if (this.hivePropertiesFile != null) {
                Properties properties = FileUtils.getProperties(this.hivePropertiesFile);
                this.hiveProperties = new HiveProperties(properties);
            } else {
                this.hiveProperties = new HiveProperties();
            }
            if (this.hivePrefix == null) {
                this.hivePrefix = this.hiveProperties.getHivePrefix();
            } else {
                 this.hiveProperties.setHivePrefix(this.hivePrefix);
            }
            if (this.hiveTable == null) {
                this.hiveTable = this.hiveProperties.getHiveTable();
            } else {
                 this.hiveProperties.setHiveTable(this.hiveTable);
            }
            if (this.partitionColumns == null) {
                this.partitionColumns = this.hiveProperties.getPartitionColumns();
            } else {
                this.hiveProperties.setPartitionColumns(this.partitionColumns);
            }
            if (this.partitionValues == null) {
                this.partitionValues = this.hiveProperties.getPartitionValues();
            } else {
                this.hiveProperties.setPartitionValues(this.partitionValues);
            }
            if (this.outputPath == null) {
                this.outputPath = this.hiveProperties.getOutputPath();
            } else {
                this.hiveProperties.setOutputPath(this.outputPath);
            }
            StringUtils.logToStdOut(this.threadName, "JobJar: " + this.jobJar);
            StringUtils.logToStdOut(this.threadName, "hivePrefix: " + this.hivePrefix);
            StringUtils.logToStdOut(this.threadName, "hiveTable: " + this.hiveTable);
            StringUtils.logToStdOut(this.threadName, "partitionColumns: " + this.partitionColumns.toString());
            StringUtils.logToStdOut(this.threadName, "partitionValues: " + this.partitionValues.toString());
            StringUtils.logToStdOut(this.threadName, "ouputPath: " + this.outputPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void runSequenceExplode(String[] args) throws IOException {
        StringUtils.logToStdOut(this.threadName, "Starting the Explode Unique job.");
        JobConf conf = (JobConf) this.getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ":");
        conf.setJobName("ExplodeUnique: " + this.partitionValues);
        conf.setMapperClass(ExplodeUniqueMapperSequence.class);
        conf.setNumReduceTasks(0);
        conf.setNumMapTasks(5);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(SequenceFileInputFormat.class);
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
            SequenceFileInputFormat.setInputPaths(conf, this.inputPaths);
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
    }

    public void runTextExplode(String[] args) throws IOException {
        StringUtils.logToStdOut(this.threadName, "Starting the Explode Unique job.");
        JobConf conf = (JobConf) this.getConf();
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
            StringUtils.logToStdOut(this.threadName, "outPath: " + outPath.getName());
            Path[] inPaths = this.hiveProperties.getFileList(conf);

            if (FileUtils.isHDFSFileExists(outPath, conf)) {
                StringUtils.logToStdOut(this.threadName, "output file path: " + this.outputPath + " exists.");
                if (!FileUtils.deleteHDFSFile(outPath, conf)) {
                    StringUtils.logToStdOut(this.threadName, "output file path: " + this.outputPath + " cannot be deleted. Abort!");
                    System.exit(1);
                }
                StringUtils.logToStdOut(this.threadName, "Deleted: " + this.outputPath);
            }
            FileInputFormat.setInputPaths(conf, inPaths);
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
    }

    public int run(String[] args) throws IOException {
        if (this.runCmd.equalsIgnoreCase("Sequence")) {
            runSequenceExplode(args);
        } else {
            runTextExplode(args);
        }
        return 0;
    }

    public static void setExplodeUniqueProps(ExplodeUnique explodeUnique, String[] args) throws Exception {
        if (args.length > 0) {
            for (int i = 0; i<args.length; i++) {
                if (args[i].equals("--hivePropertiesFile")) {
                    explodeUnique.hivePropertiesFile = args[i+1];
                }
                if (args[i].equals("--jobJar")) {
                    explodeUnique.jobJar = args[i+1];
                }
                if (args[i].equals("--myJson")) {
                    explodeUnique.jsonFile = args[i+1];
                }
                if (args[i].equals("--run")) {
                    explodeUnique.runCmd = args[i+1];
                }
                if (args[i].equals("--partitionValues")) {
                    explodeUnique.partitionValues = StringUtils.splitTrim(args[i+1], ",");
                }
                if (args[i].equals("--partitionColumns")) {
                    explodeUnique.partitionColumns = StringUtils.splitTrim(args[i+1], ",");
                }
                if (args[i].equals("--hivePrefix")) {
                    explodeUnique.hivePrefix = args[i+1];
                }
                if (args[i].equals("--hiveTable")) {
                    explodeUnique.hiveTable = args[i+1];
                }
                if (args[i].equals("--outputPath")) {
                    explodeUnique.outputPath = args[i+1];
                }
            }
        }
    }

    public static void main(String[] args) {
        InputStream is = FileUtils.getInputStream("log4j.properties");
        if (is != null) {
            StringUtils.logToStdOut("main", "Setting log4j properties");
            PropertyConfigurator.configure(is);
        }
        try {
            ExplodeUnique explodeUnique = new ExplodeUnique();
            ExplodeUnique.setExplodeUniqueProps(explodeUnique, args);
            explodeUnique.setup();
            JobConf conf = new JobConf();
            ToolRunner.run(conf, explodeUnique, args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
