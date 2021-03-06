package com.grooveshark.hadoop.jobs;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import java.lang.reflect.Constructor;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Properties;

import com.google.gson.JsonElement;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.FileAppender;


import com.grooveshark.hadoop.entities.HadoopJob;
import com.grooveshark.util.FileUtils;
import com.grooveshark.util.StringUtils;
import com.grooveshark.util.db.HiveProperties;


public class JobIssuer
{
    public static Logger log = Logger.getLogger(JobIssuer.class);
    public String jobJar;
    public String jsonFile;
    public String jsonElement;
    public String hivePrefix;
    public String hiveTable;
    public String hivePropertiesFile;
    public LinkedList<String> partitionColumns = new LinkedList<String>();
    public LinkedList<String> partitionValues = new LinkedList<String>();
    public String inputPaths = "";
    public String outputPath = "";
    public String className = JobIssuer.class.getSimpleName();
    public String jobName;
    public String jobClass;
    public String runCmd = "";
    public String[] args;
    public HiveProperties hiveProperties = null;
    public JsonElement baseElement = null;

    public void setup() {
        try {
            if (this.jsonFile != null) {
                String[] base = new String[1];
                base[0] = this.jsonElement;
                this.baseElement = FileUtils.parseJson(this.jsonFile, base);
                String jobJarPath = FileUtils.getJsonValue(this.baseElement, "jobJar");
                if (!jobJarPath.isEmpty()) {
                    this.jobJar = jobJarPath;
                }
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
            StringUtils.logToStdOut(this.className, "JobJar: " + this.jobJar);
            StringUtils.logToStdOut(this.className, "hivePrefix: " + this.hivePrefix);
            StringUtils.logToStdOut(this.className, "hiveTable: " + this.hiveTable);
            StringUtils.logToStdOut(this.className, "partitionColumns: " + this.partitionColumns.toString());
            StringUtils.logToStdOut(this.className, "partitionValues: " + this.partitionValues.toString());
            StringUtils.logToStdOut(this.className, "ouputPath: " + this.outputPath);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void setOutputPath(JobConf conf) throws IOException {
        Path outPath = new Path(this.outputPath);
        StringUtils.logToStdOut(this.className, "outPath: " + outPath.getName());
        if (FileUtils.isHDFSFileExists(outPath, conf)) {
            StringUtils.logToStdOut(this.className, "output file path: " + this.outputPath + " exists.");
            if (!FileUtils.deleteHDFSFile(outPath, conf)) {
                StringUtils.logToStdOut(this.className, "output file path: " + this.outputPath + " cannot be deleted. Abort!");
                System.exit(1);
            }
            StringUtils.logToStdOut(this.className, "Deleted: " + this.outputPath);
        }
        FileOutputFormat.setOutputPath(conf, outPath);
    }

    public <T extends HadoopJob> void runJob() throws Exception {
        Constructor<T> jobConstructor = (Constructor<T>) Class.forName(this.jobClass).getConstructor(JobIssuer.class);
        T hadoopJob = jobConstructor.newInstance(this);
        hadoopJob.executeJob();
    }

    public void setProps(String[] args) throws Exception {
        this.args = args;
        if (args.length > 0) {
            for (int i = 0; i<args.length; i++) {
                if (args[i].equals("--jobClass")) {
                    this.jobClass = args[i+1];
                }
                if (args[i].equals("--hivePropertiesFile")) {
                    this.hivePropertiesFile = args[i+1];
                }
                if (args[i].equals("--jobJar")) {
                    this.jobJar = args[i+1];
                }
                if (args[i].equals("--myJson")) {
                    this.jsonFile = args[i+1];
                }
                if (args[i].equals("--jsonElement")) {
                    this.jsonElement = args[i+1];
                }
                if (args[i].equals("--run")) {
                    this.runCmd = args[i+1];
                }
                if (args[i].equals("--partitionValues")) {
                    this.partitionValues = StringUtils.splitTrim(args[i+1], ",");
                }
                if (args[i].equals("--partitionColumns")) {
                    this.partitionColumns = StringUtils.splitTrim(args[i+1], ",");
                }
                if (args[i].equals("--hivePrefix")) {
                    this.hivePrefix = args[i+1];
                }
                if (args[i].equals("--hiveTable")) {
                    this.hiveTable = args[i+1];
                }
                if (args[i].equals("--outputPath")) {
                    this.outputPath = args[i+1];
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
            JobIssuer issuer = new JobIssuer();
            issuer.setProps(args);
            issuer.setup();
            issuer.runJob();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
