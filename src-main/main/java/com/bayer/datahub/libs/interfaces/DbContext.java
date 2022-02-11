package com.bayer.datahub.libs.interfaces;

import org.apache.avro.generic.GenericRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * For establishing connections and interacting with different
 * database sinks/sources. Instances of this class can only be obtained
 * via the <code>DbContextFactory</code> and it's <code>getContext</code> method.
 * The factory method accepts a properties object with all of the
 * necessary configuration information. Refer to the DbConfigs
 * for a list of all of the necessary configurations.
 */
public interface DbContext {
    /**
     * Connects to the database.
     */
    void connect();

    /**
     * Returns connection string used to connect
     */
    String getConnectionString();

    /**
     * Safely closes the connection to the database. Essentially a wrapper
     * around the close method of the jdbc interface
     */
    void close();

    /**
     * Batches records to be saved to database in a single transaction
     *
     * @param records A collection of apache.avro generic records i.e. from a kafka topic
     */
    void saveRecords(List<GenericRecord> records);

    /**
     * Returns a ResultSet from a select all query on the configured database and table
     *
     * @return The result of a select all query on the configured table
     */
    ResultSet getRecords();

    ResultSet getDeltaRecords(String deltaColumn, String min, String max, boolean minIncluded);

    /**
     * Returns a ResultSet from a particular query. Essentially wraps the jdbc Statement.executeQuery()
     *
     * @param query The query to execute. This should be some SELECT statement.
     */
    ResultSet getRecords(String query);

    /**
     * Returns a ResultSet from a particular query with values in column col in range from min to max
     */
    ResultSet getRecordsInRange(String inputQuery, String col, String min, String max);

    /**
     * Returns a ResultSet from a particular query with values in column col greater than min
     */
    ResultSet getRecordsWithValueGreater(String inputQuery, String col, String min);

    /**
     * Returns a ResultSet from a particular query with values in column col less than max
     */
    ResultSet getRecordsWithValueLess(String inputQuery, String col, String max);

    /**
     * @return The list of column names associated with the configured database and table
     */
    List<String> getColumnNames() throws SQLException;

    /**
     * @return The name of the currently configured table as a String
     */
    String getTable();

    /**
     * @return The number of records in the configured table
     */
    int getRowCount();

    /**
     * @return The jdbc connection object associated with this context
     */
    Connection getConnection();

    /**
     * @return configured fetch size (default is 0)
     */
    int getFetchSize();

    Optional<Date> getMinValueInColumn(String column);

    Optional<Date> getMaxValueInColumn(String column);

    String getColumnType(String columnName);
}
