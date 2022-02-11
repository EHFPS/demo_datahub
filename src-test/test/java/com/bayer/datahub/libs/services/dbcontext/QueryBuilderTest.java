package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.libs.interfaces.IQueryBuilder;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.DB_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;

class QueryBuilderTest {
    private static final String TABLE = "THE_TABLE";
    private static final String SCHEMA = "THE_SCHEMA";
    private final IQueryBuilder builder = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_TABLE, "the_table"))
            .build().getInstance(PostgresQueryBuilder.class);

    @Test
    void buildSelectAllQuery() {
        var query = builder.buildSelectAllQuery("columnA", "columnB");
        assertThat(query, equalTo("select columnA, columnB from the_table"));
    }

    @Test
    void buildSelectQueryWithMinValue() {
        var query = builder.buildRangeQueryGreaterThan("select columnA from the_table", "columnA", "100");
        assertThat(query, equalTo("select columnA from the_table where columnA >= 100"));
    }

    @Test
    void buildSelectQueryWithMaxValue() {
        var query = builder.buildRangeQueryLessThan("select columnA from the_table", "columnA", "100");
        assertThat(query, equalTo("select columnA from the_table where columnA <= 100"));
    }

    @Test
    void buildSelectQueryWithRange() {
        var query = builder.buildRangeQuery("select columnA from the_table", "columnA", "10", "100");
        assertThat(query, equalTo("select columnA from the_table where columnA >= 10 and columnA <= 100"));
    }

    @Test
    void buildUpsertQuery() {
        var key = "the_key";
        var fields = Arrays.asList("columnA", "columnB");
        var query = builder.buildUpsertQuery(fields, key);
        assertThat(query, equalTo("insert into the_table(columnA, columnB) values (?, ?) " +
                "on conflict (the_key) do update set columnA = ?, columnB = ?"));
    }

    @Test
    void buildDeltaQuery() {
        var deltaColumn = "columnA";
        assertThat(builder.buildDeltaQuery(deltaColumn, true, true, false),
                equalTo("select * from the_table where columnA > ? and columnA <= ?"));
        assertThat(builder.buildDeltaQuery(deltaColumn, false, true, false),
                equalTo("select * from the_table where columnA <= ?"));
        assertThat(builder.buildDeltaQuery(deltaColumn, true, false, false),
                equalTo("select * from the_table where columnA > ?"));
        assertThat(builder.buildDeltaQuery(deltaColumn, false, false, false),
                equalTo("select * from the_table"));
        assertThat(builder.buildDeltaQuery(deltaColumn, true, true, true),
                equalTo("select * from the_table where columnA >= ? and columnA <= ?"));
        assertThat(builder.buildDeltaQuery(deltaColumn, false, true, true),
                equalTo("select * from the_table where columnA <= ?"));
        assertThat(builder.buildDeltaQuery(deltaColumn, true, false, true),
                equalTo("select * from the_table where columnA >= ?"));
        assertThat(builder.buildDeltaQuery(deltaColumn, false, false, true),
                equalTo("select * from the_table"));
    }

    @Test
    void populateQuery() throws SQLException {
        var fieldA = "fieldA";
        var fieldB = "fieldB";
        var fieldC = "fieldC";
        var fieldD = "fieldD";
        try (var conn = H2Helper.createConn(SCHEMA, TABLE,
                "fieldA VARCHAR", "fieldB DATE", "fieldC TIMESTAMP", "fieldD INTEGER");
             var ps = conn.prepareStatement("select * from the_schema.the_table " +
                     "where fieldA > ? and fieldB > ? and fieldC > ? and fieldD > ?")) {

            var schema = SchemaBuilder.record("the_record")
                    .namespace("the_namespace")
                    .fields()
                    .name(fieldA).type().stringType().noDefault()
                    .name(fieldB).type().stringType().noDefault()
                    .name(fieldC).type().unionOf().stringType().and().intType().endUnion().noDefault()
                    .name(fieldD).type().intType().noDefault()
                    .endRecord();

            GenericRecord genericRecord = new GenericData.Record(schema);
            genericRecord.put(fieldA, "valueA");
            genericRecord.put(fieldB, "2019-06-01");
            genericRecord.put(fieldC, "2020-07-01 00:00:00");
            genericRecord.put(fieldD, 123);

            var schemaTable = new SchemaTable();
            var columns = Arrays.asList(fieldA, fieldB, fieldC, fieldD);
            schemaTable.initialize(schema, columns, fieldA);

            builder.populateQuery(ps, genericRecord, schemaTable);
            assertThat(ps.toString(), endsWith("select * from the_schema.the_table " +
                    "where fieldA > ? and fieldB > ? and fieldC > ? and fieldD > ? " +
                    "{1: 'valueA', 2: '2019-06-01', 3: '2020-07-01 00:00:00', 4: 123}"));
        }
    }

    @Test
    void minValueInColumnQuery() {
        var query = builder.minValueInColumnQuery("columnA");
        assertThat(query, equalTo("select min(columnA) from the_table"));
    }

    @Test
    void maxValueInColumnQuery() {
        var query = builder.maxValueInColumnQuery("columnA");
        assertThat(query, equalTo("select max(columnA) from the_table"));
    }
}
