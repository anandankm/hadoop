package com.grooveshark.hadoop.jobs;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.lib.db.DBInputFormat;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.hadoop.filecache.DistributedCache;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

import java.net.URL;
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
import com.grooveshark.hadoop.mappers.SplitTabMapper;
import com.grooveshark.hadoop.mappers.Neo4jUsersMapper;
import com.grooveshark.hadoop.mappers.ExplodeUniqueMapperSequence;
import com.grooveshark.hadoop.reducers.ExplodeUniqueReducer;
import com.grooveshark.hadoop.reducers.Neo4jUsersReducer;
import com.grooveshark.hadoop.reducers.Neo4jUsersFollowersReducer;
import com.grooveshark.hadoop.reducers.SplitTabReducer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.FileAppender;

public class RestNeo4j extends Configured implements Tool
{
    public static Logger log = Logger.getLogger(RestNeo4j.class);
    public String jobJar;
    public String jsonFile;
    public String hivePrefix;
    public String hiveTable;
    public String hivePropertiesFile;
    public LinkedList<String> partitionColumns;
    public LinkedList<String> partitionValues;
    public String neo4jDBPath = "";
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

    public void runCopy() throws IOException {
        JobConf conf = (JobConf) this.getConf();
        conf.setJobName("RestNeo4j: " + this.partitionValues);
        conf.setMapperClass(SplitTabMapper.class);
        conf.setReducerClass(SplitTabReducer.class);
        conf.setNumReduceTasks(5);
        conf.setNumMapTasks(5);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(LongWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        try {
            Path[] inPaths = this.hiveProperties.getFileList(conf);
            for(Path p : inPaths) {
                StringUtils.logToStdOut(this.threadName, "Inpath: " + p.toString());
            }
            Path outPath = new Path(this.outputPath);
            StringUtils.logToStdOut(this.threadName, "outPath: " + outPath.getName());
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

    public void setOutputPath(JobConf conf) throws IOException {
        Path outPath = new Path(this.outputPath);
        StringUtils.logToStdOut(this.threadName, "outPath: " + outPath.getName());
        if (FileUtils.isHDFSFileExists(outPath, conf)) {
            StringUtils.logToStdOut(this.threadName, "output file path: " + this.outputPath + " exists.");
            if (!FileUtils.deleteHDFSFile(outPath, conf)) {
                StringUtils.logToStdOut(this.threadName, "output file path: " + this.outputPath + " cannot be deleted. Abort!");
                System.exit(1);
            }
            StringUtils.logToStdOut(this.threadName, "Deleted: " + this.outputPath);
        }
        FileOutputFormat.setOutputPath(conf, outPath);
    }

    public int runUsers(String[] args) throws IOException {
        StringUtils.logToStdOut(this.threadName, "Starting the RestNeo4j job.");
        JobConf conf = (JobConf) this.getConf();
        conf.setJobName("RestNeo4j: " + this.partitionValues);
        conf.setMapperClass(Neo4jUsersMapper.class);
        conf.setReducerClass(Neo4jUsersReducer.class);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        try {
            Path[] inPaths = this.hiveProperties.getFileList(conf);
            for(Path p : inPaths) {
                StringUtils.logToStdOut(this.threadName, "Inpath: " + p.toString());
            }
            FileInputFormat.setInputPaths(conf, inPaths);
            conf.set("neo4j_db_path", this.neo4jDBPath);
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

    public int run(String[] args) throws IOException {
        StringUtils.logToStdOut(this.threadName, "Starting the RestNeo4j job.");
        JobConf conf = (JobConf) this.getConf();
        conf.setJobName("RestNeo4j: " + this.partitionValues);
        conf.setMapperClass(SplitTabMapper.class);
        conf.setReducerClass(Neo4jUsersFollowersReducer.class);
        conf.setNumReduceTasks(10);

        conf.setMapOutputKeyClass(LongWritable.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        try {
            /*
            for (URL url : GenericOptionsParser.getLibJars(conf)) {
                StringUtils.logToStdOut(this.threadName, "libjar: " + url.toString());
            }
            */
            Path[] inPaths = this.hiveProperties.getFileList(conf);
            for(Path p : inPaths) {
                StringUtils.logToStdOut(this.threadName, "Inpath: " + p.toString());
            }
            FileInputFormat.setInputPaths(conf, inPaths);
            conf.set("neo4j_db_path", this.neo4jDBPath);
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

    public static void setProps(RestNeo4j rest, String[] args) throws Exception {
        if (args.length > 0) {
            for (int i = 0; i<args.length; i++) {
                if (args[i].equals("--hivePropertiesFile")) {
                    rest.hivePropertiesFile = args[i+1];
                }
                if (args[i].equals("--neo4jDBPath")) {
                    rest.neo4jDBPath = args[i+1];
                }
                if (args[i].equals("--jobJar")) {
                    rest.jobJar = args[i+1];
                }
                if (args[i].equals("--myJson")) {
                    rest.jsonFile = args[i+1];
                }
                if (args[i].equals("--run")) {
                    rest.runCmd = args[i+1];
                }
                if (args[i].equals("--partitionValues")) {
                    rest.partitionValues = StringUtils.splitTrim(args[i+1], ",");
                }
                if (args[i].equals("--partitionColumns")) {
                    rest.partitionColumns = StringUtils.splitTrim(args[i+1], ",");
                }
                if (args[i].equals("--hivePrefix")) {
                    rest.hivePrefix = args[i+1];
                }
                if (args[i].equals("--hiveTable")) {
                    rest.hiveTable = args[i+1];
                }
                if (args[i].equals("--outputPath")) {
                    rest.outputPath = args[i+1];
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
            RestNeo4j rest = new RestNeo4j();
            RestNeo4j.setProps(rest, args);
            rest.setup();
            JobConf conf = new JobConf();
            ToolRunner.run(conf, rest, args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
