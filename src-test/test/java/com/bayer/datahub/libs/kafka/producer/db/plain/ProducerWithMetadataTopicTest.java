package com.bayer.datahub.libs.kafka.producer.db.plain;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.H2Helper;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Disabled
public class ProducerWithMetadataTopicTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private static final String METADATA_DATE = "columnA";
    private static final String NAME_COLUMN = "columnB";
    private static final String TABLE = "the_table";
    private static Method sendRecordsMethod;
    private static String topic;
    private static String metadataTopic;
    private static Schema schema;

    @BeforeEach
    public void setup() throws NoSuchMethodException {
        topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        metadataTopic = KAFKA_CLIENT_HELPER.createRandomTopic();
        schema = SchemaBuilder.record("the_record").namespace("the_namespace").fields().name(METADATA_DATE).type()
                .stringType().noDefault().name(NAME_COLUMN).type().stringType().noDefault().endRecord();
        sendRecordsMethod = PlainDbProducer.class.getDeclaredMethod("sendRecords", List.class);
        sendRecordsMethod.setAccessible(true);
    }

    @Test
    void sameRecords() throws InvocationTargetException, IllegalAccessException {
        var producer = setupProducer(createConfigMap());
        producer.init();

        var recordList = new ArrayList<GenericRecord>();

        GenericRecord rec1 = new GenericData.Record(schema);
        rec1.put(METADATA_DATE, "2016-01-04");
        rec1.put(NAME_COLUMN, "A");
        GenericRecord rec2 = new GenericData.Record(schema);
        rec2.put(METADATA_DATE, "2016-04-01");
        rec2.put(NAME_COLUMN, "A");

        recordList.add(rec1);
        recordList.add(rec2);

        sendRecordsMethod.invoke(producer, recordList);

        GenericRecord rec3 = new GenericData.Record(schema);
        rec3.put(METADATA_DATE, "2019-06-10");
        rec3.put(NAME_COLUMN, "A");
        GenericRecord rec4 = new GenericData.Record(schema);
        rec4.put(METADATA_DATE, "2019-06-10");
        rec4.put(NAME_COLUMN, "A");

        recordList.add(rec3);
        recordList.add(rec4);

        sendRecordsMethod.invoke(producer, recordList);

        // Verify topic content
        try (var consumer = KAFKA_CLIENT_HELPER.createAvroConsumer()) {
            consumer.subscribe(singletonList(topic));
            var consumerRecords = consumer.poll(Duration.ofMillis(1000));
            assertThat(consumerRecords.count(), equalTo(4));

            var list = new ArrayList<ConsumerRecord<String, Object>>();
            for (var cons : consumerRecords) {
                list.add(cons);
            }
            var actRecord = (GenericData.Record) list.get(0).value();
            assertThat(actRecord, equalTo(rec1));

            actRecord = (GenericData.Record) list.get(1).value();
            assertThat(actRecord, equalTo(rec2));

            assertThat((GenericData.Record) list.get(2).value(), equalTo(rec3));

            assertThat((GenericData.Record) list.get(3).value(), equalTo(rec4));
        }

    }

    @Test
    void differentDateRecords() throws InvocationTargetException, IllegalAccessException {
        var recordList = new ArrayList<GenericRecord>();

        GenericRecord rec1 = new GenericData.Record(schema);
        rec1.put(METADATA_DATE, "2019-05-10");
        rec1.put(NAME_COLUMN, "A");
        GenericRecord rec2 = new GenericData.Record(schema);
        rec2.put(METADATA_DATE, "2019-06-10");
        rec2.put(NAME_COLUMN, "A");

        recordList.add(rec1);
        recordList.add(rec2);

        var producer = setupProducer(createConfigMap());
        producer.init();
        sendRecordsMethod.invoke(producer, recordList);

        GenericRecord rec3 = new GenericData.Record(schema);
        rec3.put(METADATA_DATE, "2019-06-10");
        rec3.put(NAME_COLUMN, "A");
        GenericRecord rec4 = new GenericData.Record(schema);
        rec4.put(METADATA_DATE, "2019-06-10");
        rec4.put(NAME_COLUMN, "A");

        recordList.add(rec3);
        recordList.add(rec4);

        sendRecordsMethod.invoke(producer, recordList);

        // Verify topic content
        try (var consumer = KAFKA_CLIENT_HELPER.createAvroConsumer()) {
            consumer.subscribe(singletonList(topic));
            var consumerRecords = consumer.poll(Duration.ofMillis(1000));
            assertThat(consumerRecords.count(), equalTo(4));

            var list = new ArrayList<ConsumerRecord<String, Object>>();
            for (var cons : consumerRecords) {
                list.add(cons);
            }
            var actRecord = (GenericData.Record) list.get(0).value();
            assertThat(actRecord, equalTo(rec1));

            actRecord = (GenericData.Record) list.get(1).value();
            assertThat(actRecord, equalTo(rec2));

            assertThat((GenericData.Record) list.get(2).value(), equalTo(rec3));

            assertThat((GenericData.Record) list.get(3).value(), equalTo(rec4));
        }
    }

    @Test
    void differentTimestampRecords() throws InvocationTargetException, IllegalAccessException {
        var recordList = new ArrayList<GenericRecord>();

        GenericRecord rec1 = new GenericData.Record(schema);
        rec1.put(METADATA_DATE, "2019-05-10 12:30:00");
        rec1.put(NAME_COLUMN, "A");
        GenericRecord rec2 = new GenericData.Record(schema);
        rec2.put(METADATA_DATE, "2019-06-10 12:30:00");
        rec2.put(NAME_COLUMN, "A");

        recordList.add(rec1);
        recordList.add(rec2);

        var configsProperties = createConfigMap();
        configsProperties.put(PRODUCER_DB_PLAIN_METADATA_FORMAT_DATE_PROPERTY, "yyyy-MM-dd HH:mm:ss");
        var producer = setupProducer(configsProperties);
        producer.init();

        sendRecordsMethod.invoke(producer, recordList);

        GenericRecord rec3 = new GenericData.Record(schema);
        rec3.put(METADATA_DATE, "2019-06-10 12:30:00");
        rec3.put(NAME_COLUMN, "A");
        GenericRecord rec4 = new GenericData.Record(schema);
        rec4.put(METADATA_DATE, "2019-06-10 12:30:00");
        rec4.put(NAME_COLUMN, "A");

        recordList.add(rec3);
        recordList.add(rec4);

        sendRecordsMethod.invoke(producer, recordList);

        // Verify topic content
        try (var consumer = KAFKA_CLIENT_HELPER.createAvroConsumer()) {
            consumer.subscribe(singletonList(topic));
            var consumerRecords = consumer.poll(Duration.ofMillis(1000));
            assertThat(consumerRecords.count(), equalTo(4));

            var list = new ArrayList<ConsumerRecord<String, Object>>();
            for (var cons : consumerRecords) {
                list.add(cons);
            }
            var actRecord = (GenericData.Record) list.get(0).value();
            assertThat(actRecord, equalTo(rec1));

            actRecord = (GenericData.Record) list.get(1).value();
            assertThat(actRecord, equalTo(rec2));

            assertThat((GenericData.Record) list.get(2).value(), equalTo(rec3));

            assertThat((GenericData.Record) list.get(3).value(), equalTo(rec4));
        }

    }

    public Map<String, String> createConfigMap() {
        var configMap = new HashMap<String, String>();
        configMap.put(PRODUCER_DB_DELTA_COLUMN_PROPERTY, METADATA_DATE);
        configMap.put(KAFKA_COMMON_TOPIC_PROPERTY, topic);
        configMap.put(PRODUCER_DB_PLAIN_METADATA_TOPIC_PROPERTY, metadataTopic);
        configMap.put(PRODUCER_DB_PLAIN_METADATA_DATE_PROPERTY, METADATA_DATE);
        configMap.put(DB_TABLE, TABLE);
        configMap.put(PRODUCER_DB_PLAIN_RANGE_MIN_PROPERTY, "2019-03-01");
        configMap.put(PRODUCER_DB_PLAIN_RANGE_COLUMN_PROPERTY, METADATA_DATE);
        return configMap;
    }

    public KafkaClient setupProducer(Map<String, String> configMap) {
        var h2DbFile = H2Helper.createTempH2DbFile();
        var schema = SchemaBuilder.record("the_record").namespace("the_namespace").fields().endRecord();
        return FactoryBuilder.newBuilder(configMap)
                .withAvroProducerFactory()
                .withAvroConsumerFactory()
                .withH2DbContext(h2DbFile)
                .withSchemaRegistryMock(schema)
                .build().getKafkaClient();
    }

}
