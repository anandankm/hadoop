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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.LinkedList;
import java.util.Iterator;

import com.grooveshark.util.db.MysqlAccess;
import com.grooveshark.util.FileUtils;
import com.grooveshark.util.command.CommandExecutor;

public class GeonamesLoadReducer extends MapReduceBase implements Reducer<Text, Text, NullWritable, NullWritable>
{
    private MysqlAccess mysqlAccess;
    private String mapTaskId;
    private String inputFile;
    private int noRecords = 0;
    private int noValues = 0;
    private int cityLocationsFileSize = 0;
    private JobConf jobConf;
    private String mysqlUrl = "";
    private String mysqlUser = "";
    private String mysqlPass = "";

    private StringBuilder selectUnion = new StringBuilder();
    private String cityLocationsFile = "CityLocationsFile";
    private BufferedWriter cityLocationsWriter;
    private List<String> cityList = new LinkedList<String>();

    public void configure(JobConf job)
    {
        System.out.println("Entering configure");
        this.mapTaskId = job.get("mapred.task.id");
        this.inputFile = job.get("map.input.file");
        this.mysqlUrl = job.get("mysqlUrl");
        this.mysqlUser = job.get("mysqlUser");
        this.mysqlPass = job.get("mysqlPass");
        this.setFileWriter();
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
        this.jobConf = job;
    }

    public void setFileWriter()
    {
        try {
            this.cityLocationsWriter = FileUtils.getWriter(this.cityLocationsFile);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to create a new file");
            System.exit(1);
        }
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
            this.noValues++;
            if (this.cityList.size() > 1000) {
                this.processCityLocation();
            }
            if (this.noValues%100000 == 0) {
                this.insertAndDeleteFile();
            }
        }
        this.processCityLocation();
        this.insertAndDeleteFile();
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
        this.selectUnion.append(" from Cities where City = ?");
        this.cityList.add(city);
    }

    public void processCityLocation()
    {
        if (this.cityList.size() > 0) {
            PreparedStatement ps = null;
            try {
                ps = this.mysqlAccess.getPreparedStatement(this.selectUnion.toString(), this.cityList, 2000);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Error in executing mysql query");
            }
            try {
                this.writeToCityLocations(ps);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Error in writing resultSet to local file");
            } finally {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (SQLException e) {
                        System.out.println("Error in closing preparedStatement");
                        e.printStackTrace();
                    }
                }
                this.cityList.clear();
                this.selectUnion = new StringBuilder();
            }
        } else {
            System.out.println("All records are processed");
        }
    }

    public void writeToCityLocations(PreparedStatement ps)
        throws SQLException
    {
        boolean resultExists = ps.execute();
        while (resultExists) {
            ResultSet resultSet = ps.getResultSet();
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
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    throw new SQLException("Failed to write cityInfo to file", e);
                }
                this.cityLocationsFileSize++;
            }
            resultSet.close();
            resultExists = ps.getMoreResults();
        }
    }

    public void insertAndDeleteFile()
    {
        System.out.println("Num records processed: " + this.noRecords);
        System.out.println("Num values processed: " + this.noValues);
        System.out.println("CityLocationsFileSize: " + this.cityLocationsFileSize);
        CommandExecutor executor = new CommandExecutor();
        String[] cmdArray = new String[3];
        cmdArray[0] = "wc";
        cmdArray[1] = "-l";
        cmdArray[2] = this.cityLocationsFile;
        List<String> outputList = null;
        try {
            outputList = executor.execute(cmdArray);
        } catch (Exception e) {
            System.out.println("Failed to execute command. Excpetion:\n"  + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("Output of wc -l CityLocations: " + outputList);
        if (this.cityLocationsFileSize > 0) {
            try {
                this.mysqlAccess.loadDataLocal(this.cityLocationsFile, "CityLocations", " SET TSAdded = CURRENT_TIMESTAMP");
            } catch (SQLException e ) {
                System.out.println("Failed to load data into CityLocations table");
                e.printStackTrace();
            }
            File f = new File(this.cityLocationsFile);
            if (!f.delete()) {
                System.out.println("File '" + this.cityLocationsFile + "' not deleted");
            }
            this.cityLocationsFileSize = 0;
        }
        this.setFileWriter();
    }

    public void close()
    {
        System.out.println("Num records processed: " + this.noRecords);
        System.out.println("Num values processed: " + this.noValues);
        System.out.println("CityLocationsFileSize: " + this.cityLocationsFileSize);
        System.out.println("Closing mysql connection");
        try{
            this.mysqlAccess.closeConnection();
        } catch (SQLException e ) {
            System.out.println("Failed to load data into CityLocations table");
            e.printStackTrace();
        }
        System.out.println("Mysql connection closed");
        System.out.println("Reduce successfully completed");
    }

}
