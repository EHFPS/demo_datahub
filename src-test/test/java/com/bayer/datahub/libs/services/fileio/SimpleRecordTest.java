package com.bayer.datahub.libs.services.fileio;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class SimpleRecordTest {
    private static final String FIELD_A = "fieldA";
    private static final String VALUE_A = "valueA";
    private static final String TYPE_A = "string";
    private static final String FIELD_B = "fieldB";
    private static final String VALUE_B = "123";
    private static final String TYPE_B = "int";
    private static final SimpleRecord record = new SimpleRecord.Builder()
            .setField(FIELD_A, VALUE_A, TYPE_A)
            .setField(FIELD_B, VALUE_B, TYPE_B)
            .build();

    @Test
    void getFields() {
        Map<String, ValueType> fields = record.getFields();
        assertThat(fields, aMapWithSize(2));
        assertThat(fields, hasEntry(FIELD_A, new ValueType(VALUE_A, TYPE_A)));
        assertThat(fields, hasEntry(FIELD_B, new ValueType(VALUE_B, TYPE_B)));
    }

    @Test
    void getGenericRecord() {
        Schema schema = SchemaBuilder.record("the_record")
                .fields()
                .name(FIELD_A).type().stringType().noDefault()
                .name(FIELD_B).type().intType().noDefault()
                .endRecord();
        GenericRecord genericRecord = record.getGenericRecord(schema);
        assertThat(genericRecord.toString(), equalTo("{\"fieldA\": \"valueA\", \"fieldB\": 123}"));
    }

    @Test
    void testClone() {
        SimpleRecord actRecord = SimpleRecord.clone(record);
        assertThat(record, not(sameInstance(actRecord)));
        assertThat(record, equalTo(actRecord));
    }

    @Test
    void testToString() {
        String actStr = record.toString();
        String expStr = "\n    Column: fieldA Value: valueA Data type: string" +
                "\n    Column: fieldB Value: 123 Data type: int";
        assertThat(actStr, equalTo(expStr));
    }

    @Test
    void clear() {
        SimpleRecord.Builder builder = new SimpleRecord.Builder()
                .setField(FIELD_A, VALUE_A, TYPE_A)
                .setField(FIELD_B, VALUE_B, TYPE_B);
        SimpleRecord record1 = builder.build();
        assertThat(record1.getFields(), aMapWithSize(2));

        builder.clear();

        SimpleRecord record2 = builder.build();
        assertThat(record2.getFields(), anEmptyMap());
    }
}