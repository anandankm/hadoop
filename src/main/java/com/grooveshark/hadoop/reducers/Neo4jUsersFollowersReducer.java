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

import org.neo4j.rest.graphdb.RestAPIFacade;
import org.neo4j.rest.graphdb.index.RestIndex;
import org.neo4j.rest.graphdb.entity.RestNode;
import org.neo4j.rest.graphdb.entity.RestRelationship;

import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;

import static org.neo4j.helpers.collection.MapUtil.map;

public class Neo4jUsersFollowersReducer extends MapReduceBase implements Reducer<LongWritable, Text, NullWritable, NullWritable>
{
    private String mapTaskId;
    private String dbPath;
    private int noRecords = 0;
    private JobConf jobConf;
    private RestAPIFacade restAPI = null;
    private String nodeIndexName = "users";
    private String nodeKey = "userid";
    private String relIndexName = "followers";
    private String relKey = "edge";
    private String relType = "KNOWS";
    private String relPropertyKey = "name";
    private String relPropertyValue = "follows";
    private Map<String, Object> relProps;
    private RestIndex<Node> nodeIndex;
    private RestIndex<Relationship> relIndex;
    private Text outputText = new Text();

    public void configure(JobConf job)
    {
        this.mapTaskId = job.get("mapred.task.id");
        this.jobConf = job;
        this.dbPath = job.get("neo4j_db_path");
        System.out.println("dbPath: " + this.dbPath);
        this.restAPI = new RestAPIFacade(this.dbPath);
        this.nodeIndex = this.restAPI.index().forNodes(this.nodeIndexName);
        this.relIndex = (RestIndex<Relationship>) this.restAPI.index().forRelationships(this.relIndexName);
        this.relProps = map(this.relPropertyKey, this.relPropertyValue);
    }

    public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException
    {
        long userID = key.get();
        RestNode userNode = this.createNodeRetry(userID, 1);
        while (values.hasNext()) {
            long followedUserID = Long.parseLong(values.next().toString());
            String relValue = userID + ":" + followedUserID;
            RestNode followedNode = this.createNodeRetry(followedUserID, 1);
            RestRelationship restRel =
                this.restAPI.getOrCreateRelationship(this.relIndex, this.relKey, relValue, userNode, followedNode, this.relType, this.relProps);
        }
        //this.writeOutput(key, userNode, output);
        this.noRecords++;
    }

    public RestNode createNodeRetry(long userID, int numRetry)
    {
        RestNode restNode = null;
        try {
            restNode = this.restAPI.getOrCreateNode(this.nodeIndex, this.nodeKey, userID, null);
        } catch (RuntimeException ex){
            String eMes = ex.getMessage();
            System.out.println("ErrorMsg: " + eMes);
            if (eMes.contains("Error retrieving or creating node for key")) {
                if (numRetry <= 2) {
                    numRetry++;
                    return this.createNodeRetry(userID, numRetry);
                }
            }
            throw new RuntimeException(eMes);
        }
        return restNode;
    }

    public void writeOutput(LongWritable key, RestNode userNode, OutputCollector<LongWritable, Text> output) throws IOException
    {
        Iterator<Relationship> relItr = userNode.getRelationships().iterator();
        while (relItr.hasNext()) {
            Relationship rel = (Relationship) relItr.next();
            Node startNode = rel.getStartNode();
            Node endNode = rel.getEndNode();
            String relString = rel.getId() + "\t" + startNode.getId()  + "\t" + startNode.getProperty( this.nodeKey ) + "\t" + rel.getProperty( this.relPropertyKey ) + "\t" + endNode.getId() + "\t" + endNode.getProperty( this.nodeKey );
            this.outputText.set(relString);
            output.collect(key, this.outputText);
        }
    }

    public void close()
    {
        System.out.println("Number of records from the reducer: " + this.noRecords);
    }

}
