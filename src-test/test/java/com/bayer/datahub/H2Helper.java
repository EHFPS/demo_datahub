package com.bayer.datahub;

import java.io.IOException;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static java.lang.String.format;

public class H2Helper {
    private static final Random random = new Random();

    /**
     * Create connection to an H2 database and create a schema, a table with enumerated fields.
     *
     * @param schema E.g. "the_schema".
     * @param table  E.g. "the_table".
     * @param fields Fields (name and type). E.g. "field1 VARCHAR PRIMARY KEY", "field2 DATE", "field3 TIMESTAMP".
     */
    public static Connection createConn(String h2ConnectionStr, String schema, String table, List<String> fields) {
        try {
            Class.forName("org.h2.Driver");
            Connection conn = DriverManager.getConnection(h2ConnectionStr);
            Statement statement = conn.createStatement();
            statement.executeUpdate(format("CREATE SCHEMA %s", schema));
            String fieldsStr = String.join(", ", fields);
            statement.executeUpdate(format("CREATE TABLE %s.%s (%s)", schema, table, fieldsStr));
            return conn;
        } catch (SQLException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static Connection createConn(String schema, String table, String... fields) {
        String dbName = UUID.randomUUID().toString();
        String connStr = format("jdbc:h2:mem:%s", dbName);
        return createConn(connStr, schema, table, Arrays.asList(fields));
    }

    /**
     * Run H2 in the server mode to be able to connect with another Connection.
     */
    public static Connection createConnServer(String h2DbFile, String schema, String table, String... fields) {
        String connStr = getServerConnUrl(h2DbFile);
        return createConn(connStr, schema, table, Arrays.asList(fields));
    }

    public static String getServerConnUrl(String h2DbFile) {
        return format("jdbc:h2:%s;AUTO_SERVER=TRUE", h2DbFile);
    }

    public static String createTempH2DbFile() {
        try {
            return Files.createTempFile("H2DbFile-" + random.nextInt(Integer.MAX_VALUE), ".h2")
                    .toFile().getAbsolutePath();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
