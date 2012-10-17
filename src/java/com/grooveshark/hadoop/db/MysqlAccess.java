package com.grooveshark.hadoop.db;

import java.util.Map;
import java.util.HashMap;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;

public class MysqlAccess extends DBAccess 
{
    public static Map<String, Connection> connections = new HashMap<String, Connection>();

    public static Connection getInstance(String url, String user, String pass)
        throws SQLException
    {
        String key = url + user + pass;
        if (MysqlAccess.connections.containsKey(key)) {
            return MysqlAccess.connections.get(key);
        } else {
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                return DriverManager.getConnection(url, user, pass);
            } catch (Exception e) {
                throw new SQLException("Error getting a mysql connection", e);
            }
        }
    }
}
