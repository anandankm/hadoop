package com.grooveshark.hadoop.reducers;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.JobConf;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;

import java.io.File;
import java.io.IOException;
import java.io.BufferedWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Iterator;

import com.grooveshark.util.db.MysqlAccess;
import com.grooveshark.util.FileUtils;

public class GeonamesLoadReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, NullWritable>
{
    private MysqlAccess mysqlAccess;
    private String mapTaskId;
    private String inputFile;
    private int noRecords = 0;
    private JobConf jobConf;
    private String mysqlUrl = "";
    private String mysqlUser = "";
    private String mysqlPass = "";

    private StringBuilder selectUnion = new StringBuilder();
    private String cityLocationsFile = "CityLocationsFile";
    private BufferedWriter cityLocationsWriter;

    public void configure(JobConf job)
    {
        System.out.println("Entering configure");
        this.mapTaskId = job.get("mapred.task.id");
        this.inputFile = job.get("map.input.file");
        try {
            this.cityLocationsWriter = FileUtils.getWriter(this.cityLocationsFile);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to create a new file");
            System.exit(1);
        }
        this.jobConf = job;
    }

    public void reduce(Text key, Iterator<Text> values, OutputCollector<NullWritable, NullWritable> output, Reporter reporter)
        throws IOException
    {
        ++this.noRecords;
        while(values.hasNext()) {
            String[] splits = values.next().toString().split("\t");
            String city = splits[1].trim();
            String asciiCity = splits[2].trim();
            String alternateCities = splits[3].trim();
            String latitude = splits[4].trim();
            String longitude = splits[5].trim();
            String country = splits[8].trim();
            String region = splits[10].trim();
            this.addToSelectUnion(city, region, country, latitude, longitude);
        }
        this.processCityLocation();
    }

    public void addToSelectUnion(String city, String region, String country, String latitude, String longitude)
    {
        if (this.selectUnion.length() > 0) {
            this.selectUnion.append(" UNION ");
        }
        this.selectUnion.append("SELECT CityID, '");
        this.selectUnion.append(region);
        this.selectUnion.append("' as Region, '");
        this.selectUnion.append(country);
        this.selectUnion.append("' as Country, ");
        this.selectUnion.append(latitude);
        this.selectUnion.append(" as Latitude, ");
        this.selectUnion.append(longitude);
        this.selectUnion.append(" as Longitude ");
        this.selectUnion.append(" from Cities where City = '");
        this.selectUnion.append(city);
        this.selectUnion.append("'");
    }

    public void processCityLocation()
    {
        System.out.println("Getting mysql connection.. ");
        try {
            this.mysqlAccess = new MysqlAccess(
                    this.mysqlUrl,
                    this.mysqlUser,
                    this.mysqlPass);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Error in getting a mysql connection");
            System.exit(1);
        }
        System.out.println("Processing select union query.. ");
        ResultSet cityLocationSet = null;
        try {
            cityLocationSet = this.mysqlAccess.executeQuery(this.selectUnion.toString());
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Error in executing mysql query");
        }
        System.out.println("Writing to CityLocations file.. ");
        try {
            this.writeToCityLocations(cityLocationSet);
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Error in writing resultSet to local file");
        }
    }

    public void writeToCityLocations(ResultSet resultSet)
        throws SQLException
    {
        while(resultSet.next()) {
            StringBuilder cityInfo = new StringBuilder();
            cityInfo.append(resultSet.getString(1));
            cityInfo.append("\t");
            cityInfo.append(resultSet.getString(2));
            cityInfo.append("\t");
            cityInfo.append(resultSet.getString(3));
            cityInfo.append("\t");
            cityInfo.append(resultSet.getString(4));
            cityInfo.append("\t");
            cityInfo.append(resultSet.getString(5));
            try {
                FileUtils.writeLine(this.cityLocationsWriter, cityInfo.toString());
            } catch (IOException e) {
                throw new SQLException("Failed to write cityInfo to file", e);
            }
        }
    }

    public void close()
    {
        System.out.println("Num records processed: " + this.noRecords);
        try {
            this.mysqlAccess.loadDataLocal(this.cityLocationsFile, "CityLocations", " SET TSAdded = CURRENT_TIMESTAMP");
        } catch (SQLException e ) {
            e.printStackTrace();
            System.out.println("Faile to load data into CityLocations table");
        }
        File f = new File(this.cityLocationsFile);
        if (!f.delete()) {
            System.out.println("File '" + this.cityLocationsFile + "' not deleted");
        }
    }

}
