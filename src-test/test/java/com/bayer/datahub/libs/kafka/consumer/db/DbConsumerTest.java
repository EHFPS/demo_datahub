package com.bayer.datahub.libs.kafka.consumer.db;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class DbConsumerTest extends AbstractKafkaBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbConsumerTest.class);
    private static final String COLUMN_A = "columnA";
    private static final String COLUMN_B = "columnB";
    private static final String SCHEMA = "the_schema";
    private static final String TABLE = "the_table";

    @Test
    void consume() throws SQLException {
        var kafkaClientHelper = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
        var topic = kafkaClientHelper.createRandomTopic();
        var h2DbFile = H2Helper.createTempH2DbFile();
        try (var conn = H2Helper.createConnServer(h2DbFile, SCHEMA, TABLE,
                COLUMN_A + " VARCHAR", COLUMN_B + " TIMESTAMP");
             var st = conn.createStatement()) {

            //Producer messages to the topic
            var producer = kafkaClientHelper.createAvroProducer();

            var fieldNameA = "columnA";
            var fieldNameB = "columnB";
            var schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                    .fields()
                    .name(fieldNameA).type().stringType().noDefault()
                    .name(fieldNameB).type().stringType().noDefault()
                    .endRecord();

            var fieldValueA1 = "Desk1";
            var fieldValueB1 = "2019-06-10 00:00:00";
            GenericRecord expRecord1 = new GenericData.Record(schema);
            expRecord1.put(fieldNameA, fieldValueA1);
            expRecord1.put(fieldNameB, fieldValueB1);
            var record1 = new ProducerRecord<String, Object>(topic, expRecord1);

            var fieldValueA2 = "Desk2";
            var fieldValueB2 = "2020-06-10 00:00:00";
            GenericRecord expRecord2 = new GenericData.Record(schema);
            expRecord2.put(fieldNameA, fieldValueA2);
            expRecord2.put(fieldNameB, fieldValueB2);
            var record2 = new ProducerRecord<String, Object>(topic, expRecord2);

            var fieldValueA3 = "Desk3";
            var fieldValueB3 = "2021-06-10 00:00:00";
            GenericRecord expRecord3 = new GenericData.Record(schema);
            expRecord3.put(fieldNameA, fieldValueA3);
            expRecord3.put(fieldNameB, fieldValueB3);
            var record3 = new ProducerRecord<String, Object>(topic, expRecord3);

            producer.send(record1);
            producer.send(record2);
            producer.send(record3);
            producer.close();

            //Run DbConsumer
            var overrideConsumerConfig = new Properties();
            overrideConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

            var dbConsumer = FactoryBuilder.newBuilder(Map.of(
                    CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_DB.name(),
                    KAFKA_COMMON_TOPIC_PROPERTY, topic,
                    DB_SCHEMA, SCHEMA,
                    DB_TABLE, TABLE))
                    .withAvroConsumerFactory(overrideConsumerConfig)
                    .withH2DbContext(h2DbFile)
                    .withSchemaRegistryMock(schema)
                    .build().getKafkaClient();
            dbConsumer.init();
            dbConsumer.run();
            dbConsumer.stop();

            //Assert the destination table
            var rs = st.executeQuery(format("SELECT * FROM %s.%s", SCHEMA, TABLE));
            var sb = new StringBuilder();
            while (rs.next()) {
                var columnAValue = rs.getString(1);
                var columnBValue = rs.getTimestamp(2);
                sb.append(columnAValue).append("_").append(columnBValue).append("; ");
            }
            assertThat(sb.toString(), equalTo(
                    "Desk1_2019-06-10 00:00:00.0; Desk2_2020-06-10 00:00:00.0; Desk3_2021-06-10 00:00:00.0; "));
        }
    }
}