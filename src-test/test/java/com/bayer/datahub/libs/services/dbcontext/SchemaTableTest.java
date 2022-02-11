package com.bayer.datahub.libs.services.dbcontext;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

class SchemaTableTest {
    private static final String PRIMARY_KEY_COLUMN = "columnA";
    private static final String COLUMN_B = "columnB";
    private static final String COLUMN_C = "columnC";
    private static final List<String> COLUMNS = Arrays.asList(PRIMARY_KEY_COLUMN, COLUMN_B, COLUMN_C);
    private static final Schema SCHEMA = SchemaBuilder.record("the_record")
            .namespace("the_namespace")
            .fields()
            .name(PRIMARY_KEY_COLUMN).type().stringType().noDefault()
            .name(COLUMN_B).type().intType().noDefault()
            .name(COLUMN_C).type().unionOf().stringType().and().intType().endUnion().noDefault()
            .endRecord();
    private final SchemaTable schemaTable = new SchemaTable();

    @Test
    void getColumnNames() {
        schemaTable.initialize(SCHEMA, COLUMNS, PRIMARY_KEY_COLUMN);
        var columnNames = schemaTable.getColumnNames();
        assertThat(columnNames, contains(PRIMARY_KEY_COLUMN, COLUMN_B, COLUMN_C));
    }

    @Test
    void getColumnFields() {
        schemaTable.initialize(SCHEMA, COLUMNS, PRIMARY_KEY_COLUMN);
        var columnFields = schemaTable.getColumnFields();
        var expColumnField1 = new SchemaTable.ColumnField(
                Schema.Type.STRING.toString(), PRIMARY_KEY_COLUMN, PRIMARY_KEY_COLUMN, true);
        var expColumnField2 = new SchemaTable.ColumnField(
                Schema.Type.INT.toString(), COLUMN_B, COLUMN_B, false);
        var expColumnField3 = new SchemaTable.ColumnField(
                Schema.Type.STRING.toString(), COLUMN_C, COLUMN_C, false);
        assertThat(columnFields, containsInAnyOrder(expColumnField1, expColumnField2, expColumnField3));
    }
}