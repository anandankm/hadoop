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
import com.grooveshark.hadoop.mappers.MysqlExportMapper;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.FileAppender;

public class MysqlExport extends Configured implements Tool
{
    public MysqlAccess mysqlAccess;
    public DBAccess hiveAccess;
    public DBProperties dbProps;
    public static Logger log = Logger.getLogger("MysqlExport");
    public String mysqlTable;
    public String inputQuery;
    public String countQuery;
    public String mysqlColumns;
    public String splitBy;
    public String hivePrefix;
    public String hiveTable;
    public String partition;
    public String threadName;
    public String[] args;
    public String jobJar;

    public void setup(String jsonFile) {
        try {
            this.threadName = Thread.currentThread().getName();
            this.dbProps = new DBProperties();
            this.dbProps.setDefaultsDsnFilename(jsonFile);
            String[] mysqlExport = {"mysql_export"};
            JsonElement je = this.dbProps.getJsonElement(mysqlExport);
            this.jobJar = FileUtils.getJsonValue(je, "jobJar");
            this.hivePrefix = FileUtils.getJsonValue(je, "hivePrefix");
            this.hiveTable = FileUtils.getJsonValue(je, "hiveTable");
            this.partition = FileUtils.getJsonValue(je, "partition");
            this.mysqlTable = FileUtils.getJsonValue(je, "mysqlTable");
            this.inputQuery = FileUtils.getJsonValue(je, "inputQuery");
            this.countQuery = FileUtils.getJsonValue(je, "countQuery");
            String[] mysqlDsn = {FileUtils.getJsonValue(je, "mysqlDsn")};
            this.dbProps.setMysqlDsn(mysqlDsn);
            StringUtils.logToStdOut(this.threadName, "HiveUrl: " + DBProperties.DEFAULT_HIVE_URL);
            StringUtils.logToStdOut(this.threadName, "MysqlUrl: " + this.dbProps.getMysqlURL());
            this.hiveAccess = new DBAccess();
            this.hiveAccess.setUrl(DBProperties.DEFAULT_HIVE_URL);
            this.hiveAccess.setDriver(DBProperties.DEFAULT_HIVE_DRIVER);
            this.hiveAccess.setCheckIfValid(false);
            this.hiveAccess.makeConnection();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void hiveQuery(String query) throws Exception {
        StringUtils.logToStdOut(this.threadName, "Executing: " + query);
        this.hiveAccess.executeQuery(query);
    }

    public int run(String[] args) throws IOException {
        StringUtils.logToStdOut(this.threadName, "Starting the Mysql Export job.");
        StringUtils.logToStdOut(this.threadName, "Mysql table: " + this.mysqlTable);
        StringUtils.logToStdOut(this.threadName, "Hive table: " + this.hiveTable);
        JobConf conf = (JobConf) this.getConf();
        conf.setJobName("MysqlExport: " + this.mysqlTable);
        conf.setMapperClass(MysqlExportMapper.class);
        conf.setNumReduceTasks(0);
        conf.setNumMapTasks(5);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(DBInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        try {
            StringUtils.logToStdOut(this.threadName, "Created hive and mysql access points..");
            Path hiveTablePath = new Path(this.hivePrefix + this.hiveTable);
            if (!FileUtils.isHDFSFileExists(hiveTablePath, conf)) {
                StringUtils.logToStdOut(this.threadName, "The hive table: " + this.hiveTable + " does not exist. Abort!");
                System.exit(1);
            }
            hiveTablePath = hiveTablePath.suffix("/" + this.partition.replaceAll("'",""));
            StringUtils.logToStdOut(this.threadName, "Hive Table path: " + hiveTablePath);
            if (FileUtils.isHDFSFileExists(hiveTablePath, conf)) {
                StringUtils.logToStdOut(this.threadName, "Hive Table path: " + hiveTablePath + " exists.");
                if (!FileUtils.deleteHDFSFile(hiveTablePath, conf)) {
                    StringUtils.logToStdOut(this.threadName, "Hive table path: " + hiveTablePath + " cannot be deleted. Abort!");
                    System.exit(1);
                }
                StringUtils.logToStdOut(this.threadName, "Deleted: " + hiveTablePath);
                StringUtils.logToStdOut(this.threadName, "Dropping partition: " + this.partition);
                this.hiveQuery("Alter table " + this.hiveTable + " drop if exists partition (" + this.partition + ")");
            }
            DBConfiguration.configureDB(conf, DBProperties.DEFAULT_MYSQL_DRIVER,
                    dbProps.getMysqlURL(),
                    dbProps.getMysqlUser(),
                    dbProps.getMysqlPass());
            DBInputFormat.setInput(conf, MysqlWritable.class, this.inputQuery, this.countQuery);
            FileOutputFormat.setOutputPath(conf, hiveTablePath);
            conf.set("mysql.table", this.mysqlTable);
            conf.setJar(this.jobJar);
            StringUtils.logToStdOut(this.threadName, "Writing to hive table path: " + hiveTablePath);
            long start = System.currentTimeMillis();
            JobClient.runJob(conf);
            float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
            StringUtils.logToStdOut(this.threadName, "Done ("+elapsed+" secs).");
            StringUtils.logToStdOut(this.threadName, "Adding hive partitions...");
            this.hiveQuery("Alter table " + this.hiveTable + " add if not exists partition (" + this.partition + ")");
            StringUtils.logToStdOut(this.threadName, "Updated.");
        } catch (Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
        return 0;
    }

    public static void main(String[] args)
    {
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
            MysqlExport mysqlExport = new MysqlExport();
            StringUtils.logToStdOut("main", "JsonFile: " + jsonFile);
            mysqlExport.setup(jsonFile);
            JobConf conf = new JobConf();
            ToolRunner.run(conf, mysqlExport, args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
