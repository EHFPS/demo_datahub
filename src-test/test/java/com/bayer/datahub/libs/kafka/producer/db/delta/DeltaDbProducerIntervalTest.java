package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.kafkaclientbuilder.ConsumerPropertiesGenerator;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DeltaDbProducerIntervalTest extends AbstractKafkaBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaDbProducerIntervalTest.class);

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
    private static final String VALUE_B_1 = "2019-02-10 09:45:00";
    private static final String VALUE_B_2 = "2019-02-10 15:34:00";
    private static final String VALUE_B_3 = "2019-02-11 10:17:00";
    private static final String VALUE_B_4 = "2019-02-12 11:28:00";

    @Test
    void interval() throws SQLException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var h2DbFile = H2Helper.createTempH2DbFile();
        var interval = "P1D";
        String minOverride = null;
        String maxOverride = null;
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " TIMESTAMP")) {

            //Iteration 1: empty database, empty topic
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, emptyList(), emptyList(),
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, emptyList(), emptyList());
            }

            //Iteration 2: not empty database, empty topic
            var valueAList2 = asList(VALUE_A_1, VALUE_A_2, VALUE_A_3);
            var valueBList2 = asList(VALUE_B_1, VALUE_B_2, VALUE_B_3);
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, valueAList2, valueBList2,
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, valueAList2, valueBList2);
            }

            //Iteration 3: not empty database, not empty topic
            {
                var valueAList3 = asList(VALUE_A_3, VALUE_A_4);
                var valueBList3 = asList(VALUE_B_3, VALUE_B_4);
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, valueAList3, valueBList3,
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                var expValueAList3 = asList(VALUE_A_1, VALUE_A_2, VALUE_A_3, VALUE_A_4);
                var expValueBList3 = asList(VALUE_B_1, VALUE_B_2, VALUE_B_3, VALUE_B_4);
                assertTopic(topic, expValueAList3, expValueBList3);
            }

            //Iteration 4: empty database, not empty topic
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, emptyList(), emptyList(),
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                var expValueAList4 = asList(VALUE_A_1, VALUE_A_2, VALUE_A_3, VALUE_A_4);
                var expValueBList4 = asList(VALUE_B_1, VALUE_B_2, VALUE_B_3, VALUE_B_4);
                assertTopic(topic, expValueAList4, expValueBList4);
            }
        }
    }

    @Test
    void intervalWithMinMaxOverride() throws SQLException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var h2DbFile = H2Helper.createTempH2DbFile();
        var interval = "P1D";
        var minOverride = "2019-02-10 11:30:00";
        var maxOverride = "2019-02-12 10:00:00";
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " TIMESTAMP")) {

            //Iteration 1: empty database, empty topic
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, emptyList(), emptyList(),
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, emptyList(), emptyList());
            }

            //Iteration 2: not empty database, empty topic
            var valueAList2 = asList(VALUE_A_1, VALUE_A_2, VALUE_A_3);
            var valueBList2 = asList(VALUE_B_1, VALUE_B_2, VALUE_B_3);
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, valueAList2, valueBList2,
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, asList(VALUE_A_2, VALUE_A_3), asList(VALUE_B_2, VALUE_B_3));
            }

            //Iteration 3: not empty database, not empty topic
            {
                var valueAList3 = asList(VALUE_A_3, VALUE_A_4);
                var valueBList3 = asList(VALUE_B_3, VALUE_B_4);
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, valueAList3, valueBList3,
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, asList(VALUE_A_2, VALUE_A_3), asList(VALUE_B_2, VALUE_B_3));
            }

            //Iteration 4: empty database, not empty topic
            {
                var producer = initProducerWithNRecords(conn, h2DbFile, topic, emptyList(), emptyList(),
                        minOverride, maxOverride, interval);
                producer.init();
                producer.run();
                producer.stop();
                assertTopic(topic, asList(VALUE_A_2, VALUE_A_3), asList(VALUE_B_2, VALUE_B_3));
            }
        }
    }


    private void assertTopic(String topic, List<String> valueAList, List<String> valueBList) {
        LOGGER.info(CONSUMER_GROUP_PROPERTY.toString());
        try (var consumer = KAFKA_CLIENT_HELPER.createAvroConsumer()) {
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

    private KafkaClient initProducerWithNRecords(Connection conn, String h2DbFile, String topic,
                                                 List<String> valueAList, List<String> valueBList,
                                                 String minOverride, String maxOverride, String interval)
            throws SQLException {
        var configMap = new HashMap<String, String>();
        configMap.put(CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_DELTA.name());
        configMap.put(DB_SCHEMA, SCHEMA);
        configMap.put(DB_TABLE, TABLE);
        configMap.put(PRODUCER_DB_DELTA_TYPE_PROPERTY, DeltaType.TIMESTAMP.name());
        configMap.put(PRODUCER_DB_DELTA_COLUMN_PROPERTY, COLUMN_B);
        configMap.put(KAFKA_COMMON_TOPIC_PROPERTY, topic);
        configMap.put(KAFKA_COMMON_SCHEMA_REGISTRY_URL_PROPERTY, "no_value");
        if (isNotBlank(minOverride)) {
            configMap.put(PRODUCER_DB_DELTA_MIN_OVERRIDE_PROPERTY, minOverride);
        }
        if (isNotBlank(maxOverride)) {
            configMap.put(PRODUCER_DB_DELTA_MAX_OVERRIDE_PROPERTY, maxOverride);
        }
        if (isNotBlank(interval)) {
            configMap.put(PRODUCER_DB_DELTA_SELECT_INTERVAL_PROPERTY, interval);
        }

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

        return FactoryBuilder.newBuilder(configMap)
                .withAvroProducerFactory()
                .withStringConsumerFactory()
                .withSchemaRegistryMock(schema)
                .withH2DbContext(h2DbFile)
                .build().getKafkaClient();
    }


}
