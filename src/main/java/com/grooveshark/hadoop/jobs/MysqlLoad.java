package com.grooveshark.hadoop.jobs;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
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
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

import com.grooveshark.util.FileUtils;
import com.grooveshark.util.DateUtils;
import com.grooveshark.util.db.MysqlAccess;
import com.grooveshark.hadoop.mappers.MysqlLoadMapper;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.FileAppender;

public class MysqlLoad
{
    public String name = "";
    public boolean updatePropertyBag = false;
    public String propertyBagTable = "PropertyBag";
    public MysqlAccess mysqlAccess;
    public static Logger log = Logger.getLogger("MysqlLoad");

    public class MysqlLoadWorker extends Configured implements Runnable, Tool
    {
        public String mysqlTable;
        public String hiveTable;
        public String mysqlJar = "/user/anandan.rangasamy/libs/mysql-connector-java-5.1.18-bin.jar";
        public String name;
        public boolean updatePropertyBag = false;
        public String[] args;


        public int run(String[] args)
            throws IOException
        {
            logToStdOut(this.name, "Starting the Mysql Import job.");
            JobConf conf = (JobConf) this.getConf();
            conf.setJobName("MysqlLoad: " + this.mysqlTable);
            conf.setMapperClass(MysqlLoadMapper.class);
            conf.setNumReduceTasks(0);

            conf.setOutputKeyClass(NullWritable.class);
            conf.setOutputValueClass(NullWritable.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(NullOutputFormat.class);

            try {
                Path hiveTablePath = new Path("/user/hive/warehouse/" + this.hiveTable);
                Path outputPath = new Path("/user/anandan.rangasamy/wc/" + this.hiveTable);
                if (!this.isFileExists(hiveTablePath, conf)) {
                    logToStdOut(this.name, "The hive table: " + this.hiveTable + " does not exist. Abort!");
                    System.exit(1);
                }
                if (this.isFileExists(outputPath, conf)) {
                    logToStdOut(this.name, "The output path already exists!. Deleting");
                    if (!this.deleteFile(outputPath, conf)) {
                        logToStdOut(this.name, "Output path: " + outputPath + " cannot be deleted. Abort!");
                        System.exit(1);
                    }
                    logToStdOut(this.name, "Outputpath deleted.");
                }
                FileInputFormat.setInputPaths(conf, hiveTablePath);
                DistributedCache.addArchiveToClassPath(new Path(this.mysqlJar), conf);
                conf.set("mysql.table", this.mysqlTable);
                JobClient.runJob(conf);
                logToStdOut(this.name, "Job Done!");
            } catch (IOException e ) {
                e.printStackTrace();
                System.exit(1);
            }
            return 0;
        }

        public void run()
        {
            JobConf conf = new JobConf(MysqlLoadMapper.class);
            this.name += ", " + this.mysqlTable;
            try {
                int res = ToolRunner.run(conf, this, args) ;
            } catch (Exception e) {
                e.printStackTrace();
                System.exit(1);
            }
        }


        private boolean isFileExists(Path path, JobConf conf)
            throws IOException
        {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.exists(path)) {
                return true;
            } else {
                return false;
            }
        }

        private boolean deleteFile(Path path, JobConf conf)
            throws IOException
        {
            FileSystem fs = path.getFileSystem(conf);
            if (fs.delete(path, true)) {
                return true;
            } else {
                return false;
            }
        }
    }

    public String getTableSuffix()
    {
        long millisecondsSinceEpoch = System.currentTimeMillis();
        long millisecondsInWeek = 604800000L;
        int weeksSinceEpoch = (int)(millisecondsSinceEpoch / millisecondsInWeek);

        //live table is (weeks % 2) + 1. export to the inactive table by adding 1 to the weeks
        int tableIDToExportTo = ((weeksSinceEpoch + 1) % 2) + 1;

        return "Table" + Integer.toString(tableIDToExportTo);
    }

    public void doWork(String[] args)
        throws Exception
    {
        this.mysqlAccess = new MysqlAccess();
        String tableInfo = "/res/exportTableArgs";
        String tableSuffix = this.getTableSuffix();
        if (args.length != 1) {
            log.error("You need to provide the working directory as an argument. Quitting!");
            System.exit(1);
        }
        long start = System.currentTimeMillis();
        String workingDir = args[0];
        List<String> lines = FileUtils.readFile(workingDir + tableInfo);
        Map<Thread, String> threads = new HashMap<Thread, String>();
        for (String line : lines) {
            line = line.trim();
            if (line.startsWith("#")) {
                continue;
            }
            String[] tableInfoArgs = line.split(",");
            MysqlLoadWorker worker = new MysqlLoadWorker();
            worker.mysqlTable = tableInfoArgs[1] + "_" + tableSuffix;
            worker.hiveTable = tableInfoArgs[2];
            worker.args = args;
            try {
                MysqlLoad.truncateMysqlTable(worker.mysqlTable, this.mysqlAccess);
            } catch (SQLException e) {
                log.error(this.name + ": Failed to truncate table: " + worker.mysqlTable, e);
            }
            Thread t = new Thread(worker);
            worker.name = t.getName();
            t.start();
            threads.put(t, line);
        }
        while (!threads.isEmpty()) {
            this.joinThreads(threads, tableSuffix, start);
        }
        logToStdOut(this.name,"All table exportations are done!!");
    }

    public void joinThreads(Map<Thread, String> threads, String tableSuffix, long start)
    {
        Iterator it = threads.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Thread t = (Thread) pair.getKey();
            String[] tableInfoArgs = ((String) pair.getValue()).split(",");
            if (t.isAlive()) {
                try {
                    t.join(5);
                } catch (InterruptedException e) {
                    logToStdOut(this.name, "Table " + tableInfoArgs[1] + "_" + tableSuffix + " exportation failed.");
                    e.printStackTrace();
                    continue;
                }
            }
            if (!(t.isAlive())) {
                float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
                if (this.updatePropertyBag) {
                    try {
                        MysqlLoad.updatePropertyBagTable(tableInfoArgs[0], this.mysqlAccess, tableSuffix, this.propertyBagTable);
                    } catch (SQLException e) {
                        logToStdOut(this.name, "Failed to update Property: " + tableInfoArgs[0] + " value: " + tableSuffix + "\nException:\n" + StringUtils.stringifyException(e));
                    }
                }
                logToStdOut(this.name, "Table " + tableInfoArgs[1] + "_" + tableSuffix + " exported in " + elapsed + " secs.");
                it.remove();
            }
        }
    }

    public static int truncateMysqlTable(String table, MysqlAccess mysqlAccess)
        throws SQLException
    {
        String sql = "TRUNCATE TABLE " + table;
        return mysqlAccess.executeUpdate(sql);
    }

    public static int updatePropertyBagTable(String tableID, MysqlAccess mysqlAccess, String tableSuffix, String propertyBagTable)
        throws SQLException
    {
        String sql = "UPDATE " + propertyBagTable + " set Value = '" + tableSuffix + "' WHERE Property = '" + tableID + "'";
        return mysqlAccess.executeUpdate(sql);
    }

    private static void logToStdOut(String threadName, String msg)
    {
        System.out.println(DateUtils.getNow() + ": [" + threadName + "] " + msg);
    }

    public static void main(String[] args)
    {
        PropertyConfigurator.configure("conf/log4j.properties");
        try {
            MysqlLoad mysqlLoad = new MysqlLoad();
            mysqlLoad.updatePropertyBag = true;
            mysqlLoad.name = Thread.currentThread().getName();
            mysqlLoad.doWork(args);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
