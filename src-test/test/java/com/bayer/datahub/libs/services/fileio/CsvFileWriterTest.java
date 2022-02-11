package com.bayer.datahub.libs.services.fileio;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class CsvFileWriterTest {

    @Test
    void writeGenericRecords() throws IOException {
        Path outputFile = Files.createTempFile(CsvFileWriterTest.class.getSimpleName(), ".csv");
        char valueDelimiter = ';';
        String recordDelimiter = CsvRecordDelimiter.LF.name();
        String charset = "UTF-8";
        CsvFileWriter writer = new CsvFileWriter(valueDelimiter, recordDelimiter, charset);

        String fieldNameA = "columnA";
        String fieldNameB = "columnB";
        Schema schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name(fieldNameA).type().stringType().noDefault()
                .name(fieldNameB).type().stringType().noDefault()
                .endRecord();

        String fieldValueA = "Desk" + valueDelimiter + "Table\"Big\"";
        String fieldValueB = "2019-06-10\n00:00:00\t000";
        GenericRecord expRecord = new GenericData.Record(schema);
        expRecord.put(fieldNameA, fieldValueA);
        expRecord.put(fieldNameB, fieldValueB);

        writer.open(outputFile);
        writer.writeGenericRecords(Collections.singletonList(expRecord));
        writer.close();

        String contentAct = FileUtils.readFileToString(outputFile.toFile());
        assertThat(contentAct, equalTo("columnA;columnB\n\"Desk;Table\"\"Big\"\"\";\"2019-06-10\n00:00:00\t000\"\n"));
    }

    /**
     * Bug https://bijira.intranet.cnb/browse/DAAAA-2507
     */
    @Test
    void writeGenericRecordsTabDelimiter() throws IOException {
        Path outputFile = Files.createTempFile(CsvFileWriterTest.class.getSimpleName(), ".csv");
        char valueDelimiter = '\t';
        String recordDelimiter = CsvRecordDelimiter.LF.name();
        String charset = "UTF-8";
        CsvFileWriter writer = new CsvFileWriter(valueDelimiter, recordDelimiter, charset);

        String fieldNameA = "columnA";
        String fieldNameB = "columnB";
        Schema schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name(fieldNameA).type().stringType().noDefault()
                .name(fieldNameB).type().stringType().noDefault()
                .endRecord();

        String fieldValueA = "Desk" + valueDelimiter + "Table\"Big\"";
        String fieldValueB = "2019-06-10\n00:00:00\t000";
        GenericRecord expRecord = new GenericData.Record(schema);
        expRecord.put(fieldNameA, fieldValueA);
        expRecord.put(fieldNameB, fieldValueB);

        writer.open(outputFile);
        writer.writeGenericRecords(Collections.singletonList(expRecord));
        writer.close();

        String contentAct = FileUtils.readFileToString(outputFile.toFile());
        assertThat(contentAct, equalTo("columnA\tcolumnB\n\"Desk\tTable\"\"Big\"\"\"\t\"2019-06-10\n00:00:00\t000\"\n"));
    }

    @Test
    void writeJsonRecord() throws IOException {
        String json1 = "{\"name\": \"John\", \"title\": \"Mr.\", \"age\": 30}";
        String json2 = "{\"name\": \"Mary\", \"title\": \"Ms.\", \"age\": 25}";

        Path outputFile = Files.createTempFile(CsvFileWriterTest.class.getSimpleName(), ".csv");
        char valueDelimiter = ';';
        String recordDelimiter = CsvRecordDelimiter.LF.name();
        String charset = "UTF-8";
        CsvFileWriter writer = new CsvFileWriter(valueDelimiter, recordDelimiter, charset);
        writer.open(outputFile);
        writer.writeJsonRecord(json1);
        writer.writeJsonRecord(json2);
        writer.close();

        String contentAct = FileUtils.readFileToString(outputFile.toFile());
        assertThat(contentAct, equalTo("name;title;age\nJohn;Mr.;30\nMary;Ms.;25\n"));
    }
}