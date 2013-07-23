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
import java.util.HashMap;

import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.batch.BatchRestAPI;
import org.neo4j.rest.graphdb.RestRequest;
import org.neo4j.rest.graphdb.RequestResult;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.entity.RestEntity;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;

import static org.neo4j.helpers.collection.MapUtil.map;

public class Neo4jUsersReducer extends MapReduceBase implements Reducer<LongWritable, Text, NullWritable, NullWritable>
{
    private String mapTaskId;
    private String dbPath;
    private int noRecords = 0;
    private JobConf jobConf;
    private String nodeIndexName = "users";
    private String nodeKey = "userid";
    private Map<String, Object> userProps;
    private RestIndex<Node> nodeIndex;
    private RestAPIFacade restAPI = null;
    private RestRequest restRequest = null;
    private BatchRestAPI batchRestAPI = null;
    private Text outputText = new Text();

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.jobConf = job;
        this.dbPath = job.get("neo4j_db_path");
        System.out.println("dbPath: " + this.dbPath);
        this.restAPI = new RestAPIFacade(this.dbPath);
        this.batchRestAPI = new BatchRestAPI(this.dbPath, this.restAPI);
        this.restRequest = this.batchRestAPI.getRestRequest();
        this.nodeIndex = this.batchRestAPI.index().forNodes(this.nodeIndexName);
    }

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException
    {
        System.out.println("Batch Number: " + key.get());
        while (values.hasNext()) {
            String[] userData = values.next().toString().split("\t");
            if (userData.length <= 0) {
                return;
            }
            int userid = -1;
            Map<String, Object> userProps = new HashMap<String, Object>();
            Map<String, String> indexProps = new HashMap<String, String>();
            userData[0] = userData[0].trim();
            if (userData[0].isEmpty() || userData[0].equals("NULL") || userData[0].equals("\\N")) {
                return;
            }
            try {
                userid = Integer.parseInt(userData[0]);
            } catch (NumberFormatException e) {
                e.printStackTrace();
                return;
            }
            if (userid != -1) {
                for (int i=1;i<userData.length; i++)  {
                    String userInfo = userData[i].trim();
                    if (userInfo.isEmpty() || userInfo.equals("NULL") ||  userInfo.equals("\\N")) {
                        continue;
                    }
                    this.populateProperties(userInfo, userProps, indexProps, i);
                }
            }
            this.createNodeEntry(userid, userProps, indexProps);
        }
        this.batchRestAPI.executeBatchRequest();
        this.noRecords++;
    }

    public void createNodeEntry(int userID, Map<String, Object> userProps, Map<String, String> indexProps)
    {
        Map<String, Object> nodeData = map("key", this.nodeKey, "value", userID, "properties", userProps);
        RequestResult result = this.restRequest.post(this.nodeIndex.indexPath() + "?uniqueness=get_or_create", nodeData);
        RestNode restNode = this.batchRestAPI.createRestNode(result);
        this.addPropsToIndex(restNode, indexProps, this.nodeIndex);
    }

    public <T extends PropertyContainer> void addPropsToIndex(T entity, final Map<String, String> props, RestIndex<T> index) {
        final RestEntity restEntity = (RestEntity) entity;
        String uri = restEntity.getUri();
        StringBuilder fullText = new StringBuilder("");
        for (String key : props.keySet()) {
            final Map<String, Object> data = map("key", key, "value", props.get(key), "uri", uri);
            this.restRequest.post(index.indexPath(), data);
        }
    }


    public boolean populateProperties(String userInfo, Map<String, Object> userProps, Map<String, String> indexProps, int i) {
        switch (i) {
            case 1:
                userProps.put("username", userInfo);
                indexProps.put("username", userInfo);
                break;
            case 3:
                userProps.put("fname", userInfo);
                indexProps.put("fname", userInfo);
                break;
            case 4:
                userProps.put("lname", userInfo);
                indexProps.put("lname", userInfo);
                break;
            case 5:
                userProps.put("mname", userInfo);
                indexProps.put("mname", userInfo);
                break;
            case 6:
                userProps.put("city", userInfo);
                indexProps.put("city", userInfo);
                break;
            case 7:
                userProps.put("state", userInfo);
                indexProps.put("state", userInfo);
                break;
            case 8:
                userProps.put("country", userInfo);
                indexProps.put("country", userInfo);
                break;
            case 9:
                int zip = Integer.parseInt(userInfo);
                userProps.put("zip", zip);
                break;
            case 10:
                userProps.put("email", userInfo);
                indexProps.put("email", userInfo);
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
