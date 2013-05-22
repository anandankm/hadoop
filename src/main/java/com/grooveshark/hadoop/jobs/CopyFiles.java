package com.grooveshark.hadoop.jobs;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.io.InputStream;

import com.grooveshark.util.db.MysqlAccess;
import com.grooveshark.util.FileUtils;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class CopyFiles 
{
    public static class Map extends MapReduceBase implements Mapper<Text, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Text key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException
        {
            String line = key.toString();
            String[] words = line.split(" ");
            for (String w : words) {
                word.set(w);
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>
    {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter)
            throws IOException
        {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static class CopyMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text>
    {
        public void map(LongWritable key, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter)
            throws IOException
        {
            output.collect(key, value);
        }
    }

    public static void launch(String[] args)
        throws IOException
    {
        JobConf conf = new JobConf(CopyFiles.class);
        conf.setJobName("CopyFiles");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }

    public static void copyTextFile(String[] args)
        throws Exception
    {
        JobConf conf = new JobConf(CopyFiles.class);
        conf.setJobName("Make a copy");

        conf.setMapperClass(CopyMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        conf.setNumReduceTasks(0);

        conf.setOutputKeyClass(LongWritable.class);
        conf.setOutputValueClass(Text.class);

        //conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);
        if (FileUtils.isHDFSFileExists(outPath, conf)) {
            System.out.println("File exists");
            if (!FileUtils.deleteHDFSFile(outPath, conf)) {
                System.out.println("Couldn't delete output path: " + args[1]);
                System.exit(1);
            }
            System.out.println("Output File path deleted");
        }

        FileInputFormat.setInputPaths(conf, inPath);
        FileOutputFormat.setOutputPath(conf, outPath);
        JobClient.runJob(conf);
    }

    public static Logger log = Logger.getLogger("MysqlExport");
    public static void main(String[] args)
    {
        try {
            InputStream is = CopyFiles.class.getClassLoader().getResourceAsStream("log4j.properties");
            Properties props = new Properties();
            props.load(is);
            PropertyConfigurator.configure(props);
            log.info("Starting the job..");
            long start = System.currentTimeMillis();
            //CopyFiles.launch(args);
            String[] args1 = { "/user/anandan.rangasamy/wc/in",
                              "/user/anandan.rangasamy/wc/in_cp"};
            //CopyFiles.copyTextFile(args1);
            float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
            log.info("Done ("+elapsed+" secs).");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
