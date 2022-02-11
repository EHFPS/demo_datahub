package com.bayer.datahub.libs.services.fileio;

import com.bayer.datahub.FileHelper;
import com.bayer.datahub.ResourceHelper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static com.bayer.datahub.FileHelper.createPathToTempFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

class AvroFileWriterTest {

    @Test
    void writeGenericRecords() throws IOException {
        String fieldA = "fieldA";
        String fieldB = "fieldB";
        Schema schema = SchemaBuilder.record("the_record")
                .fields()
                .name(fieldA).type().stringType().noDefault()
                .name(fieldB).type().intType().noDefault()
                .endRecord();

        GenericRecord gr1 = new GenericData.Record(schema);
        gr1.put(fieldA, "valueA1");
        gr1.put(fieldB, 11);

        GenericRecord gr2 = new GenericData.Record(schema);
        gr2.put(fieldA, "valueA2");
        gr2.put(fieldB, 22);

        List<GenericRecord> records1 = Arrays.asList(gr1, gr2);

        GenericRecord gr3 = new GenericData.Record(schema);
        gr3.put(fieldA, "valueA3");
        gr3.put(fieldB, 33);

        GenericRecord gr4 = new GenericData.Record(schema);
        gr4.put(fieldA, "valueA4");
        gr4.put(fieldB, 44);

        List<GenericRecord> records2 = Arrays.asList(gr3, gr4);

        Path path = createPathToTempFile(AvroFileWriterTest.class.getSimpleName(), ".avsc");
        FileWriter writer = new AvroFileWriter();
        writer.open(path);
        writer.writeGenericRecords(records1);
        writer.writeGenericRecords(records2);
        writer.close();

        FileReader reader = new AvroFileReader(500);
        reader.open(path);
        List<SimpleRecord> simpleRecords = reader.read();
        reader.close();

        SimpleRecord expRecord1 = new SimpleRecord.Builder()
                .setField(fieldA, "valueA1", "STRING")
                .setField(fieldB, "11", "INT")
                .build();
        SimpleRecord expRecord2 = new SimpleRecord.Builder()
                .setField(fieldA, "valueA2", "STRING")
                .setField(fieldB, "22", "INT")
                .build();
        SimpleRecord expRecord3 = new SimpleRecord.Builder()
                .setField(fieldA, "valueA3", "STRING")
                .setField(fieldB, "33", "INT")
                .build();
        SimpleRecord expRecord4 = new SimpleRecord.Builder()
                .setField(fieldA, "valueA4", "STRING")
                .setField(fieldB, "44", "INT")
                .build();

        assertThat(simpleRecords, containsInAnyOrder(expRecord1, expRecord2, expRecord3, expRecord4));
    }

    @Test
    void writeJson() throws IOException {
        String json = ResourceHelper.resourceToString(AvroFileWriterTest.class, "AvroFileWriterTest.json");

        Path path = createPathToTempFile(AvroFileWriterTest.class.getSimpleName(), ".avsc");
        FileWriter writer = new AvroFileWriter();
        writer.open(path);
        writer.writeJsonRecord(json);
        writer.close();

        String actJson = FileHelper.readFileContent(path);

        assertThat(actJson, equalTo("," + json));//TODO adding "," in the beginning is a bug
    }
}