package com.grooveshark.hadoop.db;

import java.util.List;
import java.util.LinkedList;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.sql.Types;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import com.mysql.jdbc.Statement;
import java.sql.ResultSetMetaData;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

public class MysqlAccess extends DBAccess
{
    public Connection connection = null;
    public LinkedList<Integer> stringSQLTypes = new LinkedList<Integer>(
            Arrays.asList(Types.DATE, Types.TIMESTAMP, Types.VARCHAR, Types.CHAR, Types.TIME, Types.LONGVARCHAR));
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
            System.out.println("Connection lost");
            MysqlAccess.connections.remove(url + user + pass);
            if (numRetries > 3) {
                throw new SQLException("Number of retries exceeded 3 times. Check mysql connection");
            }
            numRetries++;
            return MysqlAccess.getInstance(url, user, pass, numRetries);
        } else {
            return connection;
        }
    }

    public MysqlAccess()
        throws SQLException
    {
        this.makeConnection();
    }

    public void makeConnection()
        throws SQLException
    {
        this.connection = MysqlAccess.getInstance(BELUGA_MYSQL_URL,
                BELUGA_MYSQL_USER,
                BELUGA_MYSQL_PASS, 1);
    }

    public int executeUpdate(String sql)
        throws SQLException
    {
        this.makeConnection();
        return this.connection.createStatement().executeUpdate(sql);
    }

    public boolean[] findTypes(String table)
        throws SQLException
    {
        this.makeConnection();
        ResultSetMetaData metadata = this.connection.createStatement().executeQuery("SELECT * FROM " + table + " limit 1").getMetaData();
        int numColumns = metadata.getColumnCount();
        boolean[] stringTypes = new boolean[numColumns];
        for (int i = 1; i <= numColumns; i++) {
            if (this.stringSQLTypes.contains(metadata.getColumnType(i))) {
                stringTypes[i-1] = true;
            }
        }
        return stringTypes;
    }

    public void insertValues(List<String> values, String table)
        throws SQLException
    {
        boolean[] stringTypes = this.findTypes(table);
        StringBuffer sql = new StringBuffer("INSERT INTO " + table + " VALUES ");
        int numRows = values.size();
        for (String row : values) {
            String[] columns = row.split("\t");
            if (columns.length != stringTypes.length) {
                throw new SQLException("Row (" + row + ") to be inserted does not match number of columns");
            }
            sql.append("(");
            for (int i = 0; i < columns.length; i++) {
                // Quote the string type values
                if (stringTypes[i]) {
                    columns[i] = "'" + columns[i] + "'";
                }
                sql.append(columns[i] + ",");
            }
            sql.setCharAt(sql.length() - 1, ')');
            sql.append(",");
        }
        if (values.size() > 0) {
            sql.setCharAt(sql.length() - 1, ';');
            //System.out.println(sql);
            //this.executeUpdate(sql);
        }
    }

    public void insertViaLoad(List<String> values, String table)
        throws SQLException
    {
        StringBuilder strBuilder = new StringBuilder();
        for (String value : values) {
            strBuilder.append(value);
            strBuilder.append("\n");
        }
        ByteArrayInputStream bis = new ByteArrayInputStream(strBuilder.toString().getBytes());
        this.insertViaLoad(bis, table);
    }

    public void insertViaLoad(InputStream inputStream, String table)
        throws SQLException
    {
        this.makeConnection();
        Statement statement = (com.mysql.jdbc.Statement) this.connection.createStatement();
        String sql = "LOAD DATA LOCAL INFILE 'file.txt' INTO TABLE " + table + ";";
        statement.setLocalInfileInputStream(inputStream);
        statement.executeUpdate(sql);

    }

    public static void main(String[] args)
    {
        String[] values = {
            "401509	1000	6391	86	0.057988146136148895",
            "404009	1000	6391	226	-0.1955020164773606",
            "404849	1000	6391	75	-0.6976435563762639",
            "401529	1000	6391	78	-1.0054454605826193",
            "401489	1000	6391	104	-0.6100330547758932",
            "311389	1000	6391	89	-0.6233236405821778",
            "403229	1000	6391	55	-2.3629781702508152",
            "405189	1000	6391	53	1.0407455252360986",
            "401569	1000	6391	177	-0.2970127149787915",
            "413649	1000	6391	56	1.689024311438606"
        };
        List<String> valuesList = Arrays.asList(values);
        try {
            MysqlAccess mysqlAccess = new MysqlAccess();
            long start = System.currentTimeMillis();
            mysqlAccess.insertViaLoad(valuesList, args[0]);
            float elapsed = (System.currentTimeMillis() - start)/(float) 1000;
            System.out.println("Done ("+elapsed+" secs).");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
