package com.bayer.datahub.libs.kafka.consumer;

import com.bayer.datahub.libs.config.Configs;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;

import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

public class RecordMetadata {
    private final String partitionField;
    private final String timestampField;
    private final String offsetField;
    private final String keyField;
    private Schema schema = null;

    @Inject
    public RecordMetadata(Configs configs) {
        this.partitionField = configs.partitionFieldName;
        this.timestampField = configs.timestampFieldName;
        this.offsetField = configs.offsetFieldName;
        this.keyField = configs.keyFieldName;
    }

    public ConsumerRecords<?, GenericRecord> enrichGenericRecords(ConsumerRecords<?, GenericRecord> records) {
        GenericRecordBuilder rb = null;
        List<ConsumerRecord<Object, GenericRecord>> recordList = new LinkedList<>();
        Map<TopicPartition, List<ConsumerRecord<Object, GenericRecord>>> recordMap = new LinkedHashMap<>();

        for(var record : records) {
            if(this.schema == null) {
                this.schema = updateSchema(record.value().getSchema());
                rb = new GenericRecordBuilder(this.schema);
            }
            for(var field : record.value().getSchema().getFields()) {
                rb.set(field.name(), record.value().get(field.name()));
            }
            var tp = new TopicPartition(record.topic(), record.partition());
            var timestamp = Instant.ofEpochMilli(record.timestamp()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            rb.set(partitionField, record.partition());
            rb.set(offsetField, record.offset());
            rb.set(keyField, record.key());
            rb.set(timestampField, timestamp);
            var newRecord = new ConsumerRecord<Object, GenericRecord>(record.topic(), record.partition(), record.offset(), record.key(), rb.build());
            recordList.add(newRecord);
            recordMap.put(tp, recordList);
            for(var field : this.schema.getFields()) {
                rb.clear(field);
            }
        }

        return new ConsumerRecords<>(recordMap);
    }

    private Schema updateSchema(Schema schema) {
        List<Schema.Field> baseFields = schema.getFields().stream()
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal()))
                .collect(Collectors.toList());

        baseFields.add(new Schema.Field(partitionField, Schema.create(Schema.Type.LONG), "partition", null));
        baseFields.add(new Schema.Field(offsetField, Schema.create(Schema.Type.LONG), "offset", null));
        baseFields.add(new Schema.Field(keyField, Schema.create(Schema.Type.STRING), "message key", null));
        baseFields.add(new Schema.Field(timestampField, Schema.create(Schema.Type.STRING), "kafka timestamp", null));

        return Schema.createRecord(
                schema.getName(),
                schema.getDoc(),
                schema.getNamespace(),
                false,
                baseFields);
    }

    public ConsumerRecords<?, String> enrichStringRecords(ConsumerRecords<?, String> records) {
        List<ConsumerRecord<Object, String>> recordList = new LinkedList<>();
        Map<TopicPartition, List<ConsumerRecord<Object, String>>> recordMap = new LinkedHashMap<>();

        for (var record : records) {
            var newRecord = new JSONObject(record.value());
            var key = record.key();
            var offset = record.offset();
            var timestamp = Instant.ofEpochMilli(record.timestamp()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            var partition = record.partition();
            var tp = new TopicPartition(record.topic(), record.partition());
            newRecord.put(keyField, key);
            newRecord.put(offsetField, offset);
            newRecord.put(partitionField, partition);
            newRecord.put(timestampField, timestamp);
            var newConsumerRecord = new ConsumerRecord<Object, String>(record.topic(), record.partition(), record.offset(), record.key(), newRecord.toString());
            recordList.add(newConsumerRecord);
            recordMap.put(tp, recordList);
        }

        return new ConsumerRecords<>(recordMap);
    }
}
