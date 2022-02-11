package com.bayer.datahub;

import com.bayer.datahub.kafkaclientbuilder.ConsumerPropertiesGenerator;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.google.common.collect.Lists;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static java.lang.Math.abs;
import static java.util.Collections.singleton;
import static org.mockito.Mockito.*;

/**
 * Kafka helper class for creating producer, consumer etc.
 */
public class KafkaClientHelper {
    private static final String SCHEMA_REGISTRY_URL_CONFIG = "schema.registry.url";
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaClientHelper.class);
    private static final Random random = new Random();
    private static final SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
    private final String brokers;
    private final String schemaRegistryUrl;

    public KafkaClientHelper(String brokers, String schemaRegistryUrl) {
        this.brokers = brokers;
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    private Properties getDefaultProducerProperties() {
        var defaultProperties = new Properties();
        defaultProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        defaultProperties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        return defaultProperties;
    }

    private Properties getDefaultConsumerProperties() {
        var defaultProperties = ConsumerPropertiesGenerator.genPropertiesWithGroupId();
        defaultProperties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        defaultProperties.put(SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        defaultProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return defaultProperties;
    }

    public String createRandomTopic() {
        String topic = "topic_" + abs(random.nextInt());
        var createTopicsOptions= new CreateTopicsOptions();
        try {
            createAdminClient().createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)), createTopicsOptions).all().get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOGGER.error("Failed to create topic {}", topic , e);
            throw new RuntimeException(e);
        }
        return topic;
    }

    public KafkaProducer<String, Object> createAvroProducer() {
        Serializer<String> keySer = new StringSerializer();
        Serializer<Object> valueSer = new KafkaAvroSerializer(schemaRegistryClient);
        Properties producerProperties = getDefaultProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer.getClass().getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer.getClass().getName());
        KafkaProducer<String, Object> producer = new KafkaProducer(producerProperties);

        return spy(producer);
    }

    public ProducerFactory createAvroProducerFactory() {
        ProducerFactory factory = mock(ProducerFactory.class);
        when(factory.newInstance()).thenAnswer(invocation -> createAvroProducer());
        return factory;
    }

    public KafkaConsumer<String, Object> createAvroConsumer() {
        return createAvroConsumer(null);
    }

    public KafkaConsumer<String, Object> createAvroConsumer(Properties overrideConsumerConfig) {
        Deserializer<String> keyDes = new StringDeserializer();
        Deserializer<Object> valueDes = new KafkaAvroDeserializer(schemaRegistryClient);
        Properties consumerProperties = overrideConfig(getDefaultConsumerProperties(), overrideConsumerConfig);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDes.getClass().getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDes.getClass().getName());

        LOGGER.info("group id: {}", consumerProperties.get(ConsumerConfig.GROUP_ID_CONFIG));
        KafkaConsumer<String, Object> consumer = new KafkaConsumer(consumerProperties);
        return spy(consumer);
    }

    public ConsumerFactory createAvroConsumerFactory() {
        ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.newInstance()).thenAnswer(invocation -> createAvroConsumer());
        return consumerFactory;
    }

    public ConsumerFactory createAvroConsumerFactory(Properties overrideConsumerConfig) {
        ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.newInstance()).thenAnswer(invocation -> createAvroConsumer(overrideConsumerConfig));
        return consumerFactory;
    }

    public KafkaProducer<String, byte[]> createBinaryProducer() {
        Serializer<String> keySer = new StringSerializer();
        Serializer<byte[]> valueSer = new ByteArraySerializer();
        Properties producerProperties = getDefaultProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer.getClass().getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer.getClass().getName());
        KafkaProducer<String, byte[]> producer = new KafkaProducer(producerProperties);
        return spy(producer);
    }

    public KafkaConsumer<String, byte[]> createBinaryConsumer(Properties overrideConsumerConfig) {
        Deserializer<String> keyDes = new StringDeserializer();
        Deserializer<byte[]> valueDes = new ByteArrayDeserializer();
        Properties consumerProperties = overrideConfig(getDefaultConsumerProperties(), overrideConsumerConfig);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDes.getClass().getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDes.getClass().getName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer(consumerProperties);
        return spy(consumer);
    }

    public ConsumerFactory createBinaryConsumerFactory(Properties overrideConsumerConfig) {
        ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.newInstance()).thenAnswer(invocation -> createBinaryConsumer(overrideConsumerConfig));
        return consumerFactory;
    }

    public KafkaProducer<String, String> createStringProducer() {
        Serializer<String> keySer = new StringSerializer();
        Serializer<String> valueSer = new StringSerializer();
        Properties producerProperties = getDefaultProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySer.getClass().getName());
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSer.getClass().getName());
        KafkaProducer<String, String> producer = new KafkaProducer(producerProperties);
        return spy(producer);
    }

    public KafkaConsumer<String, String> createStringConsumer(Properties overrideConsumerConfig) {
        Deserializer<String> keyDes = new StringDeserializer();
        Deserializer<String> valueDes = new StringDeserializer();
        Properties consumerProperties = overrideConfig(getDefaultConsumerProperties(), overrideConsumerConfig);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDes.getClass().getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDes.getClass().getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer(consumerProperties);

        return spy(consumer);
    }

    public ConsumerFactory createStringConsumerFactory() {
        ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.newInstance()).thenAnswer(invocation -> createStringConsumer(new Properties()));
        return consumerFactory;
    }

    public ConsumerFactory createStringConsumerFactory(Properties overrideConsumerConfig) {
        ConsumerFactory consumerFactory = mock(ConsumerFactory.class);
        when(consumerFactory.newInstance()).thenAnswer(invocation -> createStringConsumer(overrideConsumerConfig));
        return consumerFactory;
    }

    public ProducerFactory createStringProducerFactory() {
        ProducerFactory producerFactory = mock(ProducerFactory.class);
        when(producerFactory.newInstance()).thenAnswer(invocation -> createStringProducer());
        return producerFactory;
    }

    public ProducerFactory createByteProducerFactory() {
        ProducerFactory producerFactory = mock(ProducerFactory.class);
        when(producerFactory.newInstance()).thenAnswer(invocation -> createBinaryProducer());
        return producerFactory;
    }

    public AdminClient createAdminClient() {
        var properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return AdminClient.create(properties);
    }

    public void deleteRecords(String topic, int beforeOffset) {
        try {
            TopicPartition tp = new TopicPartition(topic, 0);
            Admin admin = createAdminClient();
            RecordsToDelete recordsToDelete = RecordsToDelete.beforeOffset(beforeOffset);
            Map<TopicPartition, RecordsToDelete> deleteMap = new HashMap<>();
            deleteMap.put(tp, recordsToDelete);
            admin.deleteRecords(deleteMap).all().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static Properties overrideConfig(Properties originConfig, Properties overrideConfig) {
        Properties properties = (Properties) originConfig.clone();
        if (overrideConfig != null) {
            properties.putAll(overrideConfig);
        }
        return properties;
    }

    public SchemaRegistryClient getSchemaRegistryClient() {
        return schemaRegistryClient;
    }

    public List<ConsumerRecord<String, String>> consumeAllStringRecordsFromBeginning(String topic) {
        var properties = ConsumerPropertiesGenerator.genPropertiesWithGroupId();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = createStringConsumer(properties);
        consumer.subscribe(singleton(topic));
        var tp = new TopicPartition(topic, 0);
        var endOffsets = consumer.endOffsets(singleton(tp)).getOrDefault(tp, 0L);
        var recordAll = new ArrayList<ConsumerRecord<String, String>>();
        while (recordAll.size() < endOffsets) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            Iterable<ConsumerRecord<String, String>> topicRecords = records.records(topic);
            recordAll.addAll(Lists.newArrayList(topicRecords));
        }
        return recordAll;
    }

    public List<ConsumerRecord<String, byte[]>> consumeAllBinaryRecordsFromBeginning(String topic) {
        var properties = ConsumerPropertiesGenerator.genPropertiesWithGroupId();
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = createBinaryConsumer(properties);
        consumer.subscribe(singleton(topic));
        var tp = new TopicPartition(topic, 0);
        var endOffsets = consumer.endOffsets(singleton(tp)).getOrDefault(tp, 0L);
        var recordAll = new ArrayList<ConsumerRecord<String, byte[]>>();
        while (recordAll.size() < endOffsets) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofSeconds(1));
            Iterable<ConsumerRecord<String, byte[]>> topicRecords = records.records(topic);
            recordAll.addAll(Lists.newArrayList(topicRecords));
        }
        return recordAll;
    }

}