package com.bayer.datahub.libs.services.fileio;

import com.bayer.datahub.H2Helper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.bayer.datahub.ResourceHelper.resourceToString;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class SimpleRecordManagerTest {
    private final SimpleRecordManager manager = new SimpleRecordManager();

    @Test
    void buildSimpleRecordsFromResultSet() throws SQLException {
        String schema = "the_schema";
        String table = "the_table";
        try (Connection conn = H2Helper.createConn(schema, table,
                "fieldA VARCHAR", "fieldB INTEGER");
             Statement statement = conn.createStatement()) {
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('valueA', 123)", schema, table));
            statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('valueB', 456)", schema, table));

            ResultSet resultSet = statement.executeQuery(format("SELECT * FROM %s.%s", schema, table));
            List<SimpleRecord> simpleRecords = manager.buildSimpleRecords(resultSet);

            SimpleRecord expRecord1 = new SimpleRecord.Builder()
                    .setField("FIELDA", "valueA", "VARCHAR")
                    .setField("FIELDB", "123", "INTEGER")
                    .build();
            SimpleRecord expRecord2 = new SimpleRecord.Builder()
                    .setField("FIELDA", "valueB", "VARCHAR")
                    .setField("FIELDB", "456", "INTEGER")
                    .build();
            assertThat(simpleRecords, containsInAnyOrder(expRecord1, expRecord2));
        }
    }

    @Test
    void buildSimpleRecordFromJson() {
        String json = resourceToString(getClass(), "SimpleRecordManagerTest.json");
        SimpleRecord simpleRecord = manager.buildSimpleRecord(json);
        SimpleRecord expRecord = new SimpleRecord.Builder()
                .setField("fieldA", "valueA", "string")
                .setField("fieldB", "123", "int")
                .build();
        assertThat(simpleRecord, equalTo(expRecord));
    }

    @Test
    void fromGeneralRecords() {
        String fieldA = "fieldA";
        String fieldB = "fieldB";
        Schema schema = SchemaBuilder.record("the_record")
                .fields()
                .name(fieldA).type().stringType().noDefault()
                .name(fieldB).type().intType().noDefault()
                .endRecord();
        GenericRecord gr = new GenericData.Record(schema);
        gr.put(fieldA, "valueA");
        gr.put(fieldB, 123);

        List<GenericRecord> records = Collections.singletonList(gr);
        List<SimpleRecord> simpleRecords = manager.buildSimpleRecords(records);

        SimpleRecord expRecord = new SimpleRecord.Builder()
                .setField("fieldA", "valueA", "STRING")
                .setField("fieldB", "123", "INT")
                .build();
        assertThat(simpleRecords, contains(expRecord));
    }

    @Test
    void buildGenericRecords() {
        String fieldA = "fieldA";
        String fieldB = "fieldB";
        Schema schema = SchemaBuilder.record("the_record")
                .fields()
                .name(fieldA).type().stringType().noDefault()
                .name(fieldB).type().intType().noDefault()
                .endRecord();
        SimpleRecord expRecord1 = new SimpleRecord.Builder()
                .setField("fieldA", "valueA", "STRING")
                .setField("fieldB", "123", "INT")
                .build();
        SimpleRecord expRecord2 = new SimpleRecord.Builder()
                .setField("fieldA", "valueB", "VARCHAR")
                .setField("fieldB", "456", "INTEGER")
                .build();
        List<SimpleRecord> simpleRecords = Arrays.asList(expRecord1, expRecord2);
        List<GenericRecord> genericRecords = manager.buildGenericRecords(simpleRecords, schema);

        GenericRecord gr1 = new GenericData.Record(schema);
        gr1.put(fieldA, "valueA");
        gr1.put(fieldB, 123);

        GenericRecord gr2 = new GenericData.Record(schema);
        gr2.put(fieldA, "valueB");
        gr2.put(fieldB, 456);

        assertThat(genericRecords, containsInAnyOrder(gr1, gr2));
    }

    @Test
    void buildSchema() {
        String fieldA = "fieldA";
        String fieldB = "fieldB";
        String fieldC = "fieldC";
        String fieldD = "fieldD";
        String fieldE = "fieldE";
        SimpleRecord simpleRecord = new SimpleRecord.Builder()
                .setField(fieldA, "valueA", "STRING")
                .setField(fieldB, "123", "INT")
                .setField(fieldC, "1234567890", "BIGSERIAL")
                .setField(fieldD, "1.5", "DECIMAL")
                .setField(fieldE, "2.7", "FLOAT")
                .build();
        String doc = "the_doc";
        String recordName = "the_record";
        String namespace = "the_namespace";
        Schema actSchema = manager.buildSchema(simpleRecord, doc, recordName, namespace);

        Schema expSchema = SchemaBuilder.record(recordName).namespace(namespace)
                .doc(doc)
                .fields()
                .name(fieldA).doc("Datatype: STRING").type().unionOf().stringType().and().nullType().endUnion().noDefault()
                .name(fieldB).doc("Datatype: INT").type().unionOf().intType().and().nullType().endUnion().noDefault()
                .name(fieldC).doc("Datatype: LONG").type().unionOf().longType().and().nullType().endUnion().noDefault()
                .name(fieldD).doc("Datatype: DOUBLE").type().unionOf().doubleType().and().nullType().endUnion().noDefault()
                .name(fieldE).doc("Datatype: FLOAT").type().unionOf().floatType().and().nullType().endUnion().noDefault()
                .endRecord();

        assertThat(actSchema, equalTo(expSchema));
    }
}