package com.grooveshark.hadoop.db;

import java.util.Map;
import java.util.HashMap;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class MysqlAccess extends DBAccess 
{
    public Connection connection = null;
    public static Map<String, Connection> connections = new HashMap<String, Connection>();

    public static Connection getInstance(String url, String user, String pass, int numRetries)
        throws SQLException
    {
        Connection connection = null;
        String key = url + user + pass;
        if (MysqlAccess.connections.containsKey(key)) {
            connection = MysqlAccess.connections.get(key);
        } else {
            try {
                Class.forName("com.mysql.jdbc.Driver").newInstance();
                connection = DriverManager.getConnection(url, user, pass);
                MysqlAccess.connections.put(key, connection);
            } catch (Exception e) {
                throw new SQLException("Error getting a mysql connection", e);
            }
        }
        return checkConnection(connection, url, user, pass, numRetries);
    }

    public static Connection checkConnection(Connection connection, String url, String user, String pass, int numRetries)
        throws SQLException
    {
        if (connection == null || connection.isClosed() || !connection.isValid(0)) {
            MysqlAccess.connections.remove(url + user + pass);
            if (numRetries > 3) {
                throw new SQLException("Number of retries exceeded 3 times. Check mysql connection");
            }
            numRetries ++;
            return MysqlAccess.getInstance(url, user, pass, numRetries);
        } else {
            return connection;
        }
    }

    public int executeUpdate(String sql)
        throws SQLException
    {
        this.connection = MysqlAccess.getInstance(BELUGA_MYSQL_URL,
                BELUGA_MYSQL_USER,
                BELUGA_MYSQL_PASS, 1);
        Statement statement = this.connection.createStatement();
        return statement.executeUpdate(sql);
    }
}
