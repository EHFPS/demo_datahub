package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.libs.interfaces.DbContext;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

class BaseDbContextTest {
    private static final String SCHEMA = "the_schema";
    private static final String TABLE = "the_table";
    private static final String COLUMN_A = "ColumnA";
    private static final String COLUMN_B = "ColumnB";
    private final String h2DbFile = H2Helper.createTempH2DbFile();
    private final DbContext dbContext = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_SCHEMA, SCHEMA,
            DB_TABLE, TABLE))
            .withH2DbContext(h2DbFile)
            .build().getInstance(DbContext.class);

    @Test
    void getMinValueInColumnTimestamp() throws SQLException {
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " TIMESTAMP");
             var statement = conn.createStatement()) {
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "John", "2000-08-17 15:30:32"));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "Mary", "2010-10-25 17:45:58"));
            dbContext.connect();
            var minObjOpt = dbContext.getMinValueInColumn(COLUMN_B);
            dbContext.close();
            assertThat(minObjOpt.isPresent(), is(true));
            assertThat(minObjOpt.get().toString(), equalTo("2000-08-17 15:30:32.0"));
        }
    }

    @Test
    void getMinValueInColumnDate() throws SQLException {
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " DATE");
             var statement = conn.createStatement()) {
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "John", "2000-08-17"));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "Mary", "2010-10-25"));
            dbContext.connect();
            var minObjOpt = dbContext.getMinValueInColumn(COLUMN_B);
            dbContext.close();
            assertThat(minObjOpt.isPresent(), is(true));
            assertThat(minObjOpt.get().toString(), equalTo("2000-08-17 00:00:00.0"));
        }
    }

    @Test
    void getMaxValueInColumnDate() throws SQLException {
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " DATE");
             var statement = conn.createStatement()) {
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "John", "2000-08-17"));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "Mary", "2010-10-25"));
            dbContext.connect();
            var minObjOpt = dbContext.getMaxValueInColumn(COLUMN_B);
            dbContext.close();
            assertThat(minObjOpt.isPresent(), is(true));
            assertThat(minObjOpt.get().toString(), equalTo("2010-10-25 00:00:00.0"));
        }
    }

    @Test
    void getDeltaRecordsWithMinWithMax() {
        getDeltaRecords("2001-01-01", "2002-12-31", false,
                "Mary 2001-04-25 00:00:00.0, Mike 2002-09-01 00:00:00.0");
    }

    @Test
    void getDeltaRecordsWithMinWithMaxMinNotIncluded() {
        getDeltaRecords("2000-02-17", "2002-12-31", false,
                "Mary 2001-04-25 00:00:00.0, Mike 2002-09-01 00:00:00.0");
    }

    @Test
    void getDeltaRecordsWithMinWithMaxMinIncluded() {
        getDeltaRecords("2000-02-17", "2002-12-31", true,
                "John 2000-02-17 00:00:00.0, Mary 2001-04-25 00:00:00.0, Mike 2002-09-01 00:00:00.0");
    }

    @Test
    void getDeltaRecordsWithoutMinWithMax() {
        getDeltaRecords(null, "2002-12-31", false,
                "John 2000-02-17 00:00:00.0, Mary 2001-04-25 00:00:00.0, Mike 2002-09-01 00:00:00.0");
    }

    @Test
    void getDeltaRecordsWithMinWithoutMax() {
        getDeltaRecords("2001-01-01", null, false,
                "Mary 2001-04-25 00:00:00.0, Mike 2002-09-01 00:00:00.0, Fred 2003-11-28 00:00:00.0");
    }

    @Test
    void getDeltaRecordsWithoutMinWithoutMax() {
        getDeltaRecords(null, null, false,
                "John 2000-02-17 00:00:00.0, Mary 2001-04-25 00:00:00.0, Mike 2002-09-01 00:00:00.0, Fred 2003-11-28 00:00:00.0");
    }

    private void getDeltaRecords(String minStr, String maxStr, boolean minIncluded, String expRowsStr) {
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " DATE");
             var statement = conn.createStatement()) {
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "John", "2000-02-17"));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "Mary", "2001-04-25"));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "Mike", "2002-09-01"));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                    "Fred", "2003-11-28"));
            dbContext.connect();
            var rs = dbContext.getDeltaRecords(COLUMN_B, minStr, maxStr, minIncluded);
            List<String> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(rs.getString(1) + " " + rs.getTimestamp(2));
            }
            dbContext.close();
            var rowsStr = String.join(", ", rows);
            assertThat(rowsStr, equalTo(expRowsStr));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void getColumnType() throws SQLException {
        var columnBType = "DATE";
        try (@SuppressWarnings("unused") var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " " + columnBType)) {
            dbContext.connect();
            var columnType = dbContext.getColumnType(COLUMN_B);
            dbContext.close();
            assertThat(columnType, equalTo(columnBType));
        }
    }
}