package com.bayer.datahub.libs.kafka.consumer;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.KafkaHelper.consumeRecordsToString;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class RecordConsumerTest extends AbstractKafkaBaseTest {

    @Test
    void processRecordsAtLeastOnce() throws ExecutionException, InterruptedException {
        var topic = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl()).createRandomTopic();

        var recordValue = "the record value";
        produce(topic, recordValue);

        var consumer = FactoryBuilder.newBuilder(createConfigMap(topic))
                .withStringConsumerFactory()
                .withTestDateService()
                .withSchemaRegistryMock()
                .build()
                .getInstance(RecordConsumer.class);
        var processRecordsAtLeastOnce = new ArrayList<ConsumerRecords<?, ?>>();
        consumer.init();
        consumer.run(processRecordsAtLeastOnce::add);
        consumer.stop();

        assertThat(processRecordsAtLeastOnce, hasSize(1));
        var recordsStr = consumeRecordsToString(topic, processRecordsAtLeastOnce.get(0));
        assertThat(recordsStr, equalTo("null-" + recordValue));
    }

    @Test
    void newConsumerGroupEmptyTopic() {
        var topic = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl()).createRandomTopic();
        var consumer = FactoryBuilder.newBuilder(createConfigMap(topic))
                .withStringConsumerFactory()
                .withTestDateService()
                .withSchemaRegistryMock()
                .build()
                .getInstance(RecordConsumer.class);
        var processRecordsAtLeastOnce = new ArrayList<ConsumerRecords<?, ?>>();
        consumer.init();
        consumer.run(processRecordsAtLeastOnce::add);
        consumer.stop();

        assertThat(processRecordsAtLeastOnce, empty());
    }

    private void produce(String topic, String recordValue) throws InterruptedException, ExecutionException {
        var producer = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl()).createStringProducer();
        var record = new ProducerRecord<String, String>(topic, recordValue);
        producer.send(record).get();
    }

    private Map<String, String> createConfigMap(String topic) {
        return Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_S3.name(),
                KAFKA_CONSUMER_KEY_DESERIALIZER_PROPERTY, "the_group",
                KAFKA_COMMON_TOPIC_PROPERTY, topic);
    }
}