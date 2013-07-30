package com.grooveshark.hadoop.reducers;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.HashMap;

import com.grooveshark.jdatastruct.graph.sample.Neo4jRest;
import com.grooveshark.jdatastruct.graph.sample.Neo4jRestException;
import com.grooveshark.jdatastruct.graph.sample.entities.GEdge;
import com.grooveshark.jdatastruct.graph.sample.entities.GNode;
import com.grooveshark.jdatastruct.graph.sample.entities.Location;

import static org.neo4j.helpers.collection.MapUtil.map;

public class Neo4jUsersReducer extends MapReduceBase implements Reducer<LongWritable, Text, NullWritable, NullWritable>
{
    private String mapTaskId;
    private String dbPath;
    private int noRecords = 0;
    private JobConf jobConf;
    private Neo4jRest server;

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.jobConf = job;
        this.dbPath = job.get("neo4j_db_path");
        System.out.println("dbPath: " + this.dbPath);
        try {
            this.server = new Neo4jRest(this.dbPath, GNode.NODE_INDEX, GEdge.REL_INDEX);
        } catch (Neo4jRestException e) {
            e.printStackTrace();
            System.exit(1);
        }
        this.server.setNodeKey(GNode.USERID_KEY);
        this.server.setRelKey(GEdge.EDGE_INDEX_KEY);
        this.server.setRelProps(GEdge.REL_PROPS);
    }

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException
    {
        System.out.println("Batch Number: " + key.get());
        List<GNode> nodes = new LinkedList<GNode>();
        while (values.hasNext()) {
            String[] userData = values.next().toString().split("\t");
            if (userData.length <= 0) {
                return;
            }
            long userid = -1l;
            userData[0] = userData[0].trim();
            if (userData[0].isEmpty() || userData[0].equals("NULL") || userData[0].equals("\\N")) {
                continue;
            }
            try {
                userid = Long.parseLong(userData[0]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                continue;
            }
            if (userid != -1) {
                GNode node = new GNode(userid);
                node.location = new Location();
                for (int i=1;i<userData.length; i++)  {
                    String userInfo = userData[i].trim();
                    if (userInfo.isEmpty() || userInfo.equals("NULL") ||  userInfo.equals("\\N")) {
                        continue;
                    }
                    this.populateProperties(userInfo, node, i);
                }
            }
        }
        if (nodes.size() > 0) {
            this.server.batchInsert(nodes);
        }
        this.noRecords++;
    }

    public boolean populateProperties(String userInfo, GNode node, int i) {
        switch (i) {
            case 1:
                node.username = userInfo;
                break;
            case 3:
                node.fname = userInfo;
                break;
            case 4:
                node.lname = userInfo;
                break;
            case 5:
                node.mname = userInfo;
                break;
            case 6:
                node.location.city = userInfo;
                break;
            case 7:
                node.location.state = userInfo;
                break;
            case 8:
                node.location.country = userInfo;
                break;
            case 9:
                node.location.zip = userInfo;
                break;
            case 10:
                node.email = userInfo;
                break;
            default:
                break;
        }
        return true;
    }


    public void close()
    {
        System.out.println("Number of records from the reducer: " + this.noRecords);
    }

}
