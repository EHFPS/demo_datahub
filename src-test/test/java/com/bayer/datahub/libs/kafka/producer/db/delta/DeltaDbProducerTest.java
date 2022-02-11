package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.kafkaclientbuilder.ConsumerPropertiesGenerator;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.google.api.client.util.Lists;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.h2.jdbc.JdbcSQLSyntaxErrorException;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.producer.db.delta.DbLatestKeyConsumer.LATEST_KEY_KAFKA_HEADER;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class DeltaDbProducerTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private static final Properties CONSUMER_GROUP_PROPERTY = ConsumerPropertiesGenerator.genPropertiesWithGroupId();
    private static final String COLUMN_A = "columnA";
    private static final String COLUMN_B = "columnB";
    private static final String SCHEMA = "the_schema";
    private static final String TABLE = "the_table";
    private static final String VALUE_A_1 = "value1";
    private static final String VALUE_A_2 = "value2";
    private static final String VALUE_A_3 = "value3";
    private static final String VALUE_A_4 = "value4";
    private static final String VALUE_B_1 = "2019-02-10 00:00:00";
    private static final String VALUE_B_2 = "2019-04-18 00:00:00";
    private static final String VALUE_B_3 = "2020-04-23 00:00:00";
    private static final String VALUE_B_4 = "2020-06-29 00:00:00";
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();

    @Test
    void sendRecords() throws SQLException {
        var h2DbFile = H2Helper.createTempH2DbFile();
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " TIMESTAMP")) {

            //Iteration 1: empty database, empty topic
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, COLUMN_B, emptyList(), emptyList());
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, emptyList(), emptyList());
            }

            //Iteration 2: not empty database, empty topic
            var valueAList2 = asList(VALUE_A_1, VALUE_A_2);
            var valueBList2 = asList(VALUE_B_1, VALUE_B_2);
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, COLUMN_B, valueAList2, valueBList2);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, valueAList2, valueBList2);
            }

            //Iteration 3: not empty database, not empty topic
            var valueAList3 = asList(VALUE_A_3, VALUE_A_4);
            var valueBList3 = asList(VALUE_B_3, VALUE_B_4);
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, COLUMN_B, valueAList3, valueBList3);
                producer.init();
                producer.run();
                producer.stop();
                List<String> expValueAList3 = new ArrayList<>(valueAList2);
                expValueAList3.addAll(valueAList3);
                List<String> expValueBList3 = new ArrayList<>(valueBList2);
                expValueBList3.addAll(valueBList3);
                assertTopic(topic, expValueAList3, expValueBList3);
            }

            //Iteration 4: empty database, not empty topic
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, COLUMN_B, emptyList(), emptyList());
                producer.init();
                producer.run();
                producer.stop();
                List<String> expValueAList4 = new ArrayList<>(valueAList2);
                expValueAList4.addAll(valueAList3);
                List<String> expValueBList4 = new ArrayList<>(valueBList2);
                expValueBList4.addAll(valueBList3);
                assertTopic(topic, expValueAList4, expValueBList4);
            }
        }
    }

    /**
     * Issue: https://bijira.intranet.cnb/browse/DAAAA-2399
     */
    @Test
    void latestKey() throws SQLException {
        var dateColumn = "columnA";
        var configMap = createConfigMap(topic, dateColumn);
        var h2DbFile = H2Helper.createTempH2DbFile();
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE, dateColumn + " TIMESTAMP")) {

            //Iteration 1
            var date1 = "2020-06-10 00:00:00";
            var date2 = "2019-06-10 00:00:00";
            try (var st = conn.createStatement()) {
                st.executeUpdate(format("INSERT INTO %s.%s VALUES (TIMESTAMP '%s')", SCHEMA, TABLE, date1));
                st.executeUpdate(format("INSERT INTO %s.%s VALUES (TIMESTAMP '%s')", SCHEMA, TABLE, date2));
            }
            runDeltaDbProducer(configMap, h2DbFile);
            assertLastRecord(topic, date1, date2);

            //Iteration 2
            var date3 = "2019-08-10 00:00:00";
            var date4 = "2020-08-10 00:00:00";
            try (var st = conn.createStatement()) {
                st.executeUpdate(format("INSERT INTO %s.%s VALUES (TIMESTAMP '%s')", SCHEMA, TABLE, date3));
                st.executeUpdate(format("INSERT INTO %s.%s VALUES (TIMESTAMP '%s')", SCHEMA, TABLE, date4));
            }
            runDeltaDbProducer(configMap, h2DbFile);
            assertLastRecord(topic, date4, date4);

        }
    }

    @Test
    void deltaColumnCaseInsensitive() throws SQLException {
        var h2DbFile = H2Helper.createTempH2DbFile();
        var deltaColumn = COLUMN_B.toUpperCase();
        var deltaColumnWrongCase = deltaColumn.toLowerCase();
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", deltaColumn + " TIMESTAMP")) {
            var valueAList = asList(VALUE_A_1, VALUE_A_2);
            var valueBList = asList(VALUE_B_1, VALUE_B_2);
            var producer = initProducerWithNRecords(conn, h2DbFile, topic, deltaColumnWrongCase, valueAList, valueBList);
            producer.init();
            producer.run();
            producer.stop();
            assertTopic(topic, valueAList, valueBList);
        }
    }

    @Test
    void deltaColumnNotFound() throws SQLException {
        var h2DbFile = H2Helper.createTempH2DbFile();
        var notExistsDeltaColumn = "not_exists_column";
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " TIMESTAMP")) {
            var valueAList = asList(VALUE_A_1, VALUE_A_2);
            var valueBList = asList(VALUE_B_1, VALUE_B_2);
            var producer = initProducerWithNRecords(conn, h2DbFile, topic, notExistsDeltaColumn, valueAList, valueBList);
            producer.init();
            var e = assertThrows(RuntimeException.class, producer::run);
            var causeOfCause = e.getCause().getCause();
            assertThat(causeOfCause, instanceOf(JdbcSQLSyntaxErrorException.class));
            assertThat(causeOfCause.getMessage(), equalTo("Column \"NOT_EXISTS_COLUMN\" not found; SQL statement:\nselect max(not_exists_column) from the_schema.the_table [42122-200]"));
        }
    }

    private void assertTopic(String topic, List<String> valueAList, List<String> valueBList) {
        try (var consumer = KAFKA_CLIENT_HELPER.createAvroConsumer(CONSUMER_GROUP_PROPERTY)) {
            consumer.subscribe(singletonList(topic));
            var consumerRecords = consumer.poll(Duration.ofMillis(1000));
            assertThat(consumerRecords.count(), equalTo(valueAList.size()));
            var iterable = consumerRecords.records(topic);

            var iterator = iterable.iterator();
            for (var i = 0; i < valueAList.size(); i++) {
                var actRecord1 = (GenericData.Record) iterator.next().value();
                assertThat(actRecord1.get(COLUMN_A).toString(), equalTo(valueAList.get(i)));
                assertThat(actRecord1.get(COLUMN_B).toString(), equalTo(valueBList.get(i)));
            }
        }
    }

    private KafkaClient initProducerWithNRecords(Connection conn, String h2DbFile, String topic, String deltaColumn,
                                                 List<String> valueAList, List<String> valueBList) throws SQLException {
        if (valueAList.size() != valueBList.size()) {
            throw new IllegalArgumentException("valueAList should contains the same number of rows as valueBList");
        }

        try (var statement = conn.createStatement()) {
            for (var i = 0; i < valueAList.size(); i++) {
                statement.executeUpdate(format("INSERT INTO %s.%s VALUES ('%s', '%s')", SCHEMA, TABLE,
                        valueAList.get(i), valueBList.get(i)));
            }
        }
        var schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name(COLUMN_A).type().stringType().noDefault()
                .name(COLUMN_B).type().stringType().noDefault()
                .endRecord();
        var dbLatestKeyConsumer = mock(DbLatestKeyConsumer.class);
        return FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_DELTA.name(),
                DB_SCHEMA, SCHEMA,
                DB_TABLE, TABLE,
                PRODUCER_DB_DELTA_TYPE_PROPERTY, DeltaType.TIMESTAMP.name(),
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, deltaColumn,
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_DB_DELTA_MIN_OVERRIDE_PROPERTY, "2019-01-01 00:00:00",
                PRODUCER_DB_DELTA_MAX_OVERRIDE_PROPERTY, "2021-01-01 00:00:00"))
                .withAvroProducerFactory()
                .withStringConsumerFactory()
                .withH2DbContext(h2DbFile)
                .withSchemaRegistryMock(schema)
                .override(DbLatestKeyConsumer.class, dbLatestKeyConsumer)
                .build().getKafkaClient();
    }

    private void runDeltaDbProducer(Map<String, String> configMap, String h2DbFile) {
        var deltaDbProducer = FactoryBuilder.newBuilder(configMap)
                .withAvroProducerFactory()
                .withStringConsumerFactory()
                .withH2DbContext(h2DbFile)
                .override(SchemaRegistryClient.class, KAFKA_CLIENT_HELPER.getSchemaRegistryClient())
                .build().getKafkaClient();
        deltaDbProducer.init();
        deltaDbProducer.run();
        deltaDbProducer.stop();
    }

    private ConsumerRecord<String, Object> readLastRecord(String topic) {
        try (var consumer = KAFKA_CLIENT_HELPER.createAvroConsumer(CONSUMER_GROUP_PROPERTY)) {
            consumer.subscribe(singletonList(topic));
            var consumerRecords = consumer.poll(Duration.ofMillis(1000));
            List<ConsumerRecord<String, Object>> records = Lists.newArrayList(consumerRecords.records(topic));
            return records.get(records.size() - 1);
        }
    }

    private void assertLastRecord(String topic, String expLatestKeyInTopic, String expRecordKey) {
        var latestRecord = readLastRecord(topic);
        var latestKeyInTopic = new String(latestRecord.headers().lastHeader(LATEST_KEY_KAFKA_HEADER).value());
        var recordKey = latestRecord.key();
        assertThat(latestKeyInTopic, equalTo(expLatestKeyInTopic));
        assertThat(recordKey, equalTo(expRecordKey));
    }

    private Map<String, String> createConfigMap(String topic, String dateColumn) {
        return Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_DELTA.name(),
                PRODUCER_DB_DELTA_TYPE_PROPERTY, DeltaType.TIMESTAMP.name(),
                PRODUCER_DB_DELTA_COLUMN_PROPERTY, dateColumn.toUpperCase(),
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                DB_SCHEMA, SCHEMA,
                DB_TABLE, TABLE);
    }

}
