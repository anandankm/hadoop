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
import com.grooveshark.util.db.DBProperties;
import com.grooveshark.util.StringUtils;
import com.grooveshark.hadoop.mappers.GeonamesLoadMapper;
import com.grooveshark.hadoop.reducers.GeonamesLoadReducer;
import com.grooveshark.hadoop.entities.HadoopJob;


public class GeonamesLoad extends Configured implements HadoopJob, Tool
{
    public static Logger log = Logger.getLogger(GeonamesLoad.class);
    private JobIssuer jobIssuer;

    private String mysqlUrl;
    private String mysqlUser;
    private String mysqlPass;
    private String className = GeonamesLoad.class.getSimpleName();

    public GeonamesLoad(JobIssuer jobIssuer) throws Exception {
        this.jobIssuer = jobIssuer;
        DBProperties dbp = new DBProperties();
        this.mysqlUrl = dbp.getJsonMysqlUrl(this.jobIssuer.baseElement);
        this.mysqlUser = dbp.getJsonMysqlUser(this.jobIssuer.baseElement);
        this.mysqlPass = dbp.getJsonMysqlPassword(this.jobIssuer.baseElement);
    }

    public int run(String[] args) throws IOException {
        StringUtils.logToStdOut(this.className, "Starting the Geonames load job.");
        JobConf conf = (JobConf) this.getConf();
        conf.setJobName("Geonames Load: " + this.jobIssuer.partitionValues);
        conf.setMapperClass(GeonamesLoadMapper.class);
        conf.setReducerClass(GeonamesLoadReducer.class);
        conf.setNumReduceTasks(16);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(NullWritable.class);
        conf.setOutputValueClass(NullWritable.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(NullOutputFormat.class);
        try {
            Path[] inPaths = this.jobIssuer.hiveProperties.getFileList(conf);
            for(Path p : inPaths) {
                StringUtils.logToStdOut(this.className, "Inpath: " + p.toString());
            }
            FileInputFormat.setInputPaths(conf, inPaths);
            StringUtils.logToStdOut(this.className, "mysqlUrl: " + this.mysqlUrl);
            StringUtils.logToStdOut(this.className, "mysqlUser: " + this.mysqlUser);
            StringUtils.logToStdOut(this.className, "mysqlPass: " + this.mysqlPass);
            conf.set("mysqlUrl", this.mysqlUrl);
            conf.set("mysqlUser", this.mysqlUser);
            conf.set("mysqlPass", this.mysqlPass);
            conf.setJar(this.jobIssuer.jobJar);
            StringUtils.logToStdOut(this.className, "Starting hadoop job");
            long start = System.currentTimeMillis();
            JobClient.runJob(conf);
            float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
            StringUtils.logToStdOut(this.className, "Done ("+elapsed+" secs).");
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
