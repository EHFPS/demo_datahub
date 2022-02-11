package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_TOPIC_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_COLUMN_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.db.delta.DbLatestKeyConsumer.LATEST_KEY_KAFKA_HEADER;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

class DbShareDocLatestKeyConsumerTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();

    @Test
    void getLatestKeyFromKafkaHeader() throws ExecutionException, InterruptedException {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();

        var schema = SchemaBuilder.record("the_record").fields().endRecord();
        GenericRecord record = new GenericData.Record(schema);

        var lastKey1 = "2021-10-10 00:00:00";
        var lastKey2 = "2020-09-10 00:00:00";
        var lastKey3 = "2019-08-10 00:00:00";

        Iterable<Header> headers1 = singletonList(new RecordHeader(LATEST_KEY_KAFKA_HEADER, lastKey1.getBytes()));
        Iterable<Header> headers2 = singletonList(new RecordHeader(LATEST_KEY_KAFKA_HEADER, lastKey2.getBytes()));
        Iterable<Header> headers3 = singletonList(new RecordHeader(LATEST_KEY_KAFKA_HEADER, lastKey3.getBytes()));

        var partition = 0;
        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, partition, "2021-06-10 00:00:00", record, headers1)).get();
            producer.send(new ProducerRecord<>(topic, partition, "2020-06-10 00:00:00", record, headers2)).get();
            producer.send(new ProducerRecord<>(topic, partition, "2019-06-10 00:00:00", record, headers3)).get();
        }

        var configs = new Configs(Map.of(KAFKA_COMMON_TOPIC_PROPERTY, topic));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, equalTo(lastKey3));
    }

    @Test
    void getLatestKeyFromKey() throws ExecutionException, InterruptedException {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();

        var schema = SchemaBuilder.record("the_record").fields().endRecord();
        GenericRecord record = new GenericData.Record(schema);

        var key1 = "2021-06-10 00:00:00";
        var key2 = "2020-06-10 00:00:00";
        var key3 = "2019-06-10 00:00:00";
        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, key1, record)).get();
            producer.send(new ProducerRecord<>(topic, key2, record)).get();
            producer.send(new ProducerRecord<>(topic, key3, record)).get();
        }

        var configs = new Configs(Map.of(KAFKA_COMMON_TOPIC_PROPERTY, topic));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, equalTo(key3));
    }

    @Test
    void getLatestKeyFromTimestamp() throws ExecutionException, InterruptedException {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();

        var timestampField = "CHANGED_TS";
        var schema = SchemaBuilder.record("the_record").fields()
                .requiredString(timestampField)
                .endRecord();

        GenericRecord record1 = new GenericData.Record(schema);
        record1.put(timestampField, "2021-06-10 00:00:00");

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(timestampField, "2020-06-10 00:00:00");

        var timestamp3 = "2019-06-10 00:00:00";
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(timestampField, timestamp3);

        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, record1)).get();
            producer.send(new ProducerRecord<>(topic, record2)).get();
            producer.send(new ProducerRecord<>(topic, record3)).get();
        }

        var configs = new Configs(Map.of(
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, timestampField));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, equalTo(timestamp3));
    }

    @Test
    void getLatestKeyFromTimestampFieldAbsent() throws ExecutionException, InterruptedException {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();

        var timestampField = "CHANGED_TS";
        var schema = SchemaBuilder.record("the_record").fields().endRecord();

        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, new GenericData.Record(schema))).get();
            producer.send(new ProducerRecord<>(topic, new GenericData.Record(schema))).get();
            producer.send(new ProducerRecord<>(topic, new GenericData.Record(schema))).get();
        }

        var configs = new Configs(Map.of(
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, timestampField));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, nullValue());
    }

    @Test
    void getLatestKeyFromData() throws ExecutionException, InterruptedException {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();

        var dateField = "CHANGED_DATE";
        var schema = SchemaBuilder.record("the_record").fields()
                .requiredString(dateField)
                .endRecord();

        GenericRecord record1 = new GenericData.Record(schema);
        record1.put(dateField, "2021-06-10");

        GenericRecord record2 = new GenericData.Record(schema);
        record2.put(dateField, "2020-06-10");

        var date3 = "2019-06-10";
        GenericRecord record3 = new GenericData.Record(schema);
        record3.put(dateField, date3);

        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, record1)).get();
            producer.send(new ProducerRecord<>(topic, record2)).get();
            producer.send(new ProducerRecord<>(topic, record3)).get();
        }

        var configs = new Configs(Map.of(
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, dateField));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, equalTo(date3));
    }

    @Test
    void getLatestKeyFromDateFieldAbsent() throws ExecutionException, InterruptedException {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();

        var timestampField = "CHANGED_DATE";
        var schema = SchemaBuilder.record("the_record").fields().endRecord();

        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, new GenericData.Record(schema))).get();
            producer.send(new ProducerRecord<>(topic, new GenericData.Record(schema))).get();
            producer.send(new ProducerRecord<>(topic, new GenericData.Record(schema))).get();
        }

        var configs = new Configs(Map.of(
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, timestampField));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, nullValue());
    }

    @Test
    void getLatestKeyEmptyTopic() {
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();
        var configs = new Configs(Map.of(KAFKA_COMMON_TOPIC_PROPERTY, topic));
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, nullValue());
    }

    @Test
    void getLatestKeyEmptyTopicWithBeginningOffset() throws ExecutionException, InterruptedException {
        var dateField = "CHANGED_DATE";
        var schema = SchemaBuilder.record("the_record").fields()
                .requiredString(dateField)
                .endRecord();
        GenericRecord record = new GenericData.Record(schema);
        record.put(dateField, "2021-06-10");
        try (var producer = KAFKA_CLIENT_HELPER.createAvroProducer()) {
            producer.send(new ProducerRecord<>(topic, record)).get();
        }

        cleanTopic();

        var configs = new Configs(Map.of(
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, dateField));
        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory();
        var dbLatestKeyConsumer = new DbLatestKeyConsumer(configs, consumerFactory);
        var latestKey = dbLatestKeyConsumer.getLatestKey();
        assertThat(latestKey, nullValue());
    }

    private void cleanTopic() {
        KAFKA_CLIENT_HELPER.deleteRecords(topic, 1);
    }
}