package com.grooveshark.hadoop.jobs;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.IOException;

import com.grooveshark.util.FileUtils;
import com.grooveshark.util.StringUtils;
import com.grooveshark.hadoop.mappers.GeonamesLoadMapper;
import com.grooveshark.hadoop.entities.HadoopJob;


public class GeonamesLoad extends Configured implements HadoopJob, Tool
{
    public static Logger log = Logger.getLogger(GeonamesLoad.class);
    private JobIssuer jobIssuer;

    private String mysqlHost;
    private String mysqlDB;
    private String mysqlUser;
    private String mysqlPass;

    public GeonamesLoad(JobIssuer jobIssuer) {
        this.jobIssuer = jobIssuer;
        this.mysqlHost = FileUtils.getJsonValue(this.jobIssuer.baseElement, "mysql_host");
        this.mysqlDB = FileUtils.getJsonValue(this.jobIssuer.baseElement, "mysql_db");
        this.mysqlUser = FileUtils.getJsonValue(this.jobIssuer.baseElement, "mysql_user");
        this.mysqlPass = FileUtils.getJsonValue(this.jobIssuer.baseElement, "mysql_pass");
    }

    public int run(String[] args) throws IOException {
        StringUtils.logToStdOut(this.jobIssuer.threadName, "Starting the Geonames load job.");
        JobConf conf = (JobConf) this.getConf();
        conf.setJobName("Geonames Load: " + this.jobIssuer.partitionValues);
        conf.setMapperClass(GeonamesLoadMapper.class);
        conf.setNumReduceTasks(0);

        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        try {
            Path[] inPaths = this.jobIssuer.hiveProperties.getFileList(conf);
            for(Path p : inPaths) {
                StringUtils.logToStdOut(this.jobIssuer.threadName, "Inpath: " + p.toString());
            }
            FileInputFormat.setInputPaths(conf, inPaths);
            conf.set("mysqlHost", this.mysqlHost);
            conf.set("mysqlDB", this.mysqlDB);
            conf.set("mysqlUser", this.mysqlUser);
            conf.set("mysqlPass", this.mysqlPass);
            conf.setJar(this.jobIssuer.jobJar);
            StringUtils.logToStdOut(this.jobIssuer.threadName, "Starting hadoop job");
            long start = System.currentTimeMillis();
            JobClient.runJob(conf);
            float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
            StringUtils.logToStdOut(this.jobIssuer.threadName, "Done ("+elapsed+" secs).");
        } catch (Exception e ) {
            e.printStackTrace();
            System.exit(1);
        }
        return 0;
    }

    public void executeJob() {
        try {
            JobConf conf = new JobConf();
            ToolRunner.run(conf, this, this.jobIssuer.args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
