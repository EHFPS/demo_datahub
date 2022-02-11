package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;

class OracleQueryBuilderTest {
    private static final String TABLE = "the_table";
    private static final String SCHEMA = "the_schema";
    private final OracleQueryBuilder builder = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_TABLE, TABLE,
            DB_SCHEMA, SCHEMA))
            .build().getInstance(OracleQueryBuilder.class);

    @Test
    void buildUpsertQuery() {
        var fields = Arrays.asList("fieldA", "fieldB");
        var key = "fieldA";
        var actQuery = builder.buildUpsertQuery(fields, key);
        var expQuery = "merge into the_schema.the_table using (select 1 from dual) on (fieldA = ?) " +
                "when matched then update set fieldB = ? when not matched then insert (fieldA, fieldB) values (?, ?)";
        assertThat(actQuery, equalTo(expQuery));
    }

    @Test
    void buildRowCountQuery() {
        var actQuery = builder.buildRowCountQuery();
        var expQuery = "select count(*) from the_schema.the_table";
        assertThat(actQuery, equalTo(expQuery));
    }

    @Test
    void buildSelectAllQuery() {
        var actQuery = builder.buildSelectAllQuery();
        var expQuery = "select * from the_schema.the_table";
        assertThat(actQuery, equalTo(expQuery));
    }

    @Test
    void setString() throws SQLException, ClassNotFoundException {
        ClassLoader.getSystemClassLoader().loadClass(org.h2.Driver.class.getName());
        try (var conn = H2Helper.createConn(SCHEMA, TABLE,
                "string_field VARCHAR", "date_field DATE", "timestamp_field TIMESTAMP");
             var ps = conn.prepareStatement("select * from the_schema.the_table " +
                     "where string_field > ? and date_field > ? and timestamp_field > ?")) {

            var stringValue = "the_value";
            builder.setString(ps, 1, stringValue);

            var dateValue = "2019-06-01";
            builder.setString(ps, 2, dateValue);

            var timestampValue = "2019-02-14 15:31:14";
            builder.setString(ps, 3, timestampValue);

            var actQuery = ps.toString();
            var expQuery = "select * from the_schema.the_table " +
                    "where string_field > ? and date_field > ? and timestamp_field > ? " +
                    "{1: 'the_value', 2: DATE '2019-06-01', 3: TIMESTAMP '2019-02-14 15:31:14'}";
            assertThat(actQuery, endsWith(expQuery));
        }
    }

    /**
     * For bug "DAAAA-1889 Lose milliseconds in OracleQueryBuilder".
     */
    @Test
    void setStringLosesMilliseconds() throws SQLException {
        try (var conn = H2Helper.createConn(SCHEMA, TABLE, "timestamp_field TIMESTAMP");
             var ps = conn.prepareStatement(
                     "select * from the_schema.the_table where timestamp_field > ?")) {

            var timestampValue = "2019-02-14 15:31:14.123";
            builder.setString(ps, 1, timestampValue);

            var actQuery = ps.toString();
            var expQuery = "select * from the_schema.the_table where timestamp_field > ? " +
                    "{1: TIMESTAMP '2019-02-14 15:31:14.123'}";
            assertThat(actQuery, endsWith(expQuery));
        }
    }

    @Test
    void rangeQuerySelectAll() {
        var actQuery = builder.buildRangeQuery("", "col", "'2020-02-02'", "'2021-02-02'");
        var expQuery = "select * from the_schema.the_table where col >= DATE '2020-02-02' and col <= DATE '2021-02-02'";
        assertThat(actQuery, equalTo(expQuery));
    }

    @Test
    void rangeQuerySelectAllGreaterThan() {
        var actQuery = builder.buildRangeQueryGreaterThan("", "col", "'2020-02-02'");
        var expQuery = "select * from the_schema.the_table where col >= DATE '2020-02-02'";
        assertThat(actQuery, equalTo(expQuery));
    }

    @Test
    void rangeQuerySelectAllLessThan() {
        var actQuery = builder.buildRangeQueryLessThan("", "col", "'2021-02-02'");
        var expQuery = "select * from the_schema.the_table where col <= DATE '2021-02-02'";
        assertThat(actQuery, equalTo(expQuery));
    }
}
