package com.bayer.datahub.libs.kafka;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.SchemaRegistryError;
import com.bayer.datahub.libs.services.fileio.SimpleRecord;
import com.bayer.datahub.libs.services.fileio.SimpleRecordManager;
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryService;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RecordService {
    private static final Logger log = LoggerFactory.getLogger(RecordService.class);
    private final SimpleRecordManager simpleRecordManager;
    private final String topic;
    private final SchemaRegistryService schemaRegistryService;
    private Schema schema;

    @Inject
    public RecordService(Configs configs, SchemaRegistryService schemaRegistryService,
                         SimpleRecordManager simpleRecordManager) {
        this.topic = configs.kafkaCommonTopic;
        this.schemaRegistryService = schemaRegistryService;
        this.simpleRecordManager = simpleRecordManager;
        log.info("Configured topic: " + this.topic);
    }

    public List<GenericRecord> simpleToGenericRecords(List<SimpleRecord> simpleRecords) {
        log.debug("Converting SimpleRecord to GenericRecord: {} records...", simpleRecords.size());
        List<GenericRecord> records = new ArrayList<>();
        try {
            records = simpleRecordManager.buildGenericRecords(simpleRecords, getSchema());
        } catch (SchemaRegistryError e) {
            if (!simpleRecords.isEmpty()) {
                var schema = simpleRecordManager.buildSchema(
                        simpleRecords.get(0), "Schema for topic: " + topic, topic, "DATAHUB");
                log.info("Schema created: " + schema);
                records = simpleRecordManager.buildGenericRecords(simpleRecords, schema);
            } else {
                log.info("There are no records");
            }
        }
        log.debug("Converted SimpleRecord to GenericRecord: {} records", records.size());
        return records;
    }

    public GenericRecord simpleToGenericRecord(SimpleRecord simpleRecord) {
        GenericRecord record;
        try {
            record = simpleRecordManager.buildGenericRecord(simpleRecord, getSchema());
        } catch (SchemaRegistryError e) {
            var schema = simpleRecordManager.buildSchema(simpleRecord,
                    "Schema for topic: " + topic, topic, "DATAHUB");
            log.info("Schema created: " + schema);
            record = simpleRecordManager.buildGenericRecord(simpleRecord, schema);
        }
        return record;
    }

    private Schema getSchema() throws SchemaRegistryError {
        if (schema == null) {
            log.debug("Retrieving Avro schema from Schema Registry...");
            schema = schemaRegistryService.getSchema();
            log.debug("Retrieved Avro schema from Schema Registry: {}", schema);
        }
        return schema;
    }

    public List<GenericRecord> consumerRecordsToGenericRecords(ConsumerRecords<?, GenericRecord> records) {
        var genericRecords = new LinkedList<GenericRecord>();
        for (var record : records) {
            genericRecords.add(record.value());
        }
        return genericRecords;
    }

    public String consumerRecordsToCsv(ConsumerRecords<?, String> records) {
        var sb = new StringBuilder();
        for (var record : records) {
            sb.append(record.value()).append("\n");
        }
        return sb.toString();
    }

    public List<String> consumerRecordsToStringArray(ConsumerRecords<?, String> records) {
        var stringRecords = new LinkedList<String>();
        for (var record : records) {
            stringRecords.add(record.value());
        }
        return stringRecords;
    }

    public String convertGenericRecordToJson(GenericRecord genericRecord) {
        try {
            var schema = genericRecord.getSchema();
            var datumWriter = new GenericDatumWriter<GenericRecord>(schema);
            var out = new ByteArrayOutputStream();
            var encoder = EncoderFactory.get().jsonEncoder(schema, out);
            datumWriter.write(genericRecord, encoder);
            encoder.flush();
            return out.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
