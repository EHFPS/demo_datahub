package com.bayer.datahub.libs.kafka.producer.db.plain;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.exceptions.SchemaRegistryError;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import static com.bayer.datahub.ReflectionHelper.readObjectField;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PlainDbProducerTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private static final String COLUMN_A = "columnA";
    private static final String COLUMN_B = "columnB";

    private static final String VALUE_A1 = "valueA";
    private static final String VALUE_A2 = "valueB";
    private static final String VALUE_A3 = "valueC";
    private static final String VALUE_A4 = "valueD";
    private static final String VALUE_B1 = "2019-02-10";
    private static final String VALUE_B2 = "2019-04-18";
    private static final String VALUE_B3 = "2020-04-23";
    private static final String VALUE_B4 = "2020-06-29";
    private static final List<String> VALUES_A = List.of(VALUE_A1, VALUE_A2, VALUE_A3, VALUE_A4);
    private static final List<String> VALUES_B = List.of(VALUE_B1, VALUE_B2, VALUE_B3, VALUE_B4);
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();

    @Test
    void runWith4Records() throws SQLException, SchemaRegistryError {
        var producer = initProducerWithNRecords(4, false, COLUMN_B, null, null, null);
        producer.init();
        producer.run();
        producer.stop();
        KafkaProducer<String, GenericRecord> kafkaProducer = readObjectField(producer, "producer");
        verify(kafkaProducer, times(4)).send(any(ProducerRecord.class), any(Callback.class));
        assertTopic(topic, List.of(VALUE_A1, VALUE_A2, VALUE_A3, VALUE_A4), List.of(VALUE_B1, VALUE_B2, VALUE_B3, VALUE_B4));
    }

    @Test
    void runWithNoRecords() throws SQLException, SchemaRegistryError {
        var producer = initProducerWithNRecords(0, false, COLUMN_B, null, null, null);
        producer.init();
        producer.run();
        producer.stop();
        KafkaProducer<String, GenericRecord> kafkaProducer = readObjectField(producer, "producer");
        verify(kafkaProducer, times(0)).send(any(ProducerRecord.class), any(Callback.class));
        assertTopic(topic, List.of(), List.of());
    }

    @Test
    void runWith15RecordsWithFetchOf5Sends15RecordsToKafka() throws SQLException, SchemaRegistryError {
        var producer = initProducerWithNRecords(15, false, COLUMN_B, null, null, 5);
        producer.init();
        producer.run();
        producer.stop();
        KafkaProducer<String, GenericRecord> kafkaProducer = readObjectField(producer, "producer");
        verify(kafkaProducer, times(15)).send(any(ProducerRecord.class), any(Callback.class));
        assertTopic(topic,
                List.of(VALUE_A1, VALUE_A2, VALUE_A3, VALUE_A4, VALUE_A1, VALUE_A2, VALUE_A3, VALUE_A4,
                        VALUE_A1, VALUE_A2, VALUE_A3, VALUE_A4, VALUE_A1, VALUE_A2, VALUE_A3),
                List.of(VALUE_B1, VALUE_B2, VALUE_B3, VALUE_B4, VALUE_B1, VALUE_B2, VALUE_B3, VALUE_B4,
                        VALUE_B1, VALUE_B2, VALUE_B3, VALUE_B4, VALUE_B1, VALUE_B2, VALUE_B3));
    }

    @Test
    void getRecordsWithoutRange() throws SQLException, SchemaRegistryError {
        runProducerWithRange(false, COLUMN_B, null, null);
        assertTopic(topic, List.of(VALUE_A1, VALUE_A2, VALUE_A3, VALUE_A4), List.of(VALUE_B1, VALUE_B2, VALUE_B3, VALUE_B4));
    }

    @Test
    void getRecordsWithoutMinAndMaxRange() throws SQLException, SchemaRegistryError {
        runProducerWithRange(true, COLUMN_B, "2019-03-01", "2020-05-01");
        assertTopic(topic, List.of(VALUE_A2, VALUE_A3), List.of(VALUE_B2, VALUE_B3));
    }

    @Test
    void getRecordsWithMinRange() throws SQLException, SchemaRegistryError {
        runProducerWithRange(true, COLUMN_B, "2019-03-01", null);
        assertTopic(topic, List.of(VALUE_A2, VALUE_A3, VALUE_A4), List.of(VALUE_B2, VALUE_B3, VALUE_B4));
    }

    @Test
    void getRecordsWithMaxRange() throws SQLException, SchemaRegistryError {
        runProducerWithRange(true, COLUMN_B, null, "2020-05-01");
        assertTopic(topic, List.of(VALUE_A1, VALUE_A2, VALUE_A3), List.of(VALUE_B1, VALUE_B2, VALUE_B3));
    }

    @Test
    void getRecordsRangeColumnMissed() {
        var e = assertThrows(RuntimeException.class,
                () -> runProducerWithRange(true, null, null, null));
        assertThat(e, instanceOf(InvalidConfigurationException.class));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.db.plain.range.column'. Current value: ''. Please check your configuration."));
    }

    @Test
    void getRecordsRangeMinAndMaxMissed() {
        var e = assertThrows(RuntimeException.class,
                () -> runProducerWithRange(true, COLUMN_B, null, null));
        assertThat(e, instanceOf(InvalidConfigurationException.class));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.db.plain.range.min, producer.db.plain.range.max'. Current value: ', '. Please check your configuration."));
    }

    private void assertTopic(String topic, List<String> valueAList, List<String> valueBList) {
        try (KafkaConsumer<String, Object> consumer = KAFKA_CLIENT_HELPER.createAvroConsumer()) {
            consumer.subscribe(singletonList(topic));
            ConsumerRecords<String, Object> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            assertThat(consumerRecords.count(), equalTo(valueAList.size()));
            Iterable<ConsumerRecord<String, Object>> iterable = consumerRecords.records(topic);

            Iterator<ConsumerRecord<String, Object>> iterator = iterable.iterator();
            for (int i = 0; i < valueAList.size(); i++) {
                GenericData.Record actRecord1 = (GenericData.Record) iterator.next().value();
                assertThat(actRecord1.get(COLUMN_A).toString(), equalTo(valueAList.get(i)));
                assertThat(actRecord1.get(COLUMN_B).toString(), equalTo(valueBList.get(i)));
            }
        }
    }

    private void runProducerWithRange(boolean rangeEnabled, String rangeColumn, String rangeMin, String rangeMax)
            throws SQLException, SchemaRegistryError {
        var producer = initProducerWithNRecords(4, rangeEnabled, rangeColumn, rangeMin, rangeMax, null);
        producer.init();
        producer.run();
        producer.stop();
    }

    private KafkaClient initProducerWithNRecords(int num, boolean rangeEnabled, String rangeColumn, String rangeMin,
                                                 String rangeMax, Integer dbFetch) throws SQLException, SchemaRegistryError {
        var schema = "the_schema";
        var table = "the_table";

        var configMap = new HashMap<String, String>();
        configMap.put(CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_PLAIN.name());
        configMap.put(DB_SCHEMA, schema);
        configMap.put(DB_TABLE, table);
        configMap.put(PRODUCER_DB_PLAIN_RANGE_ENABLE_PROPERTY, Boolean.toString(rangeEnabled));
        configMap.put(KAFKA_COMMON_TOPIC_PROPERTY, topic);
        if (rangeColumn != null) {
            configMap.put(PRODUCER_DB_PLAIN_RANGE_COLUMN_PROPERTY, rangeColumn);
        }
        if (rangeMin != null) {
            configMap.put(PRODUCER_DB_PLAIN_RANGE_MIN_PROPERTY, rangeMin);
        }
        if (rangeMax != null) {
            configMap.put(PRODUCER_DB_PLAIN_RANGE_MAX_PROPERTY, rangeMax);
        }
        if (dbFetch != null) {
            configMap.put(DB_FETCH, dbFetch.toString());
        }

        var h2DbFile = H2Helper.createTempH2DbFile();
        try (var conn = H2Helper.createConnServer(h2DbFile, schema, table,
                COLUMN_A + " VARCHAR", COLUMN_B + " DATE");
             var statement = conn.createStatement()) {
            if (num > 0) {
                for (var i = 0; i < num; i++) {
                    statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", schema, table,
                            VALUES_A.get(i % 4), VALUES_B.get(i % 4)));
                }
                statement.close();
            }
            var schemaForProducer = SchemaBuilder.record("the_record").namespace("the_namespace")
                    .fields()
                    .name(COLUMN_A).type().stringType().noDefault()
                    .name(COLUMN_B).type().stringType().noDefault()
                    .endRecord();
            return FactoryBuilder.newBuilder(configMap)
                    .withAvroProducerFactory()
                    .withAvroConsumerFactory()
                    .withH2DbContext(h2DbFile)
                    .withSchemaRegistryMock(schemaForProducer)
                    .build().getKafkaClient();
        }
    }
}