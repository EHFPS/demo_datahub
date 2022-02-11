package com.bayer.datahub.libs.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.utility.DockerImageName;

public abstract class AbstractKafkaBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractKafkaBaseTest.class);

    static final KafkaContainer KAFKA_CONTAINER;
    static final SchemaRegistryContainer SCHEMA_REGISTRY_CONTAINER;
    static final Network NETWORK;

    static {
        NETWORK = Network.newNetwork();
        KAFKA_CONTAINER = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.4")).withNetwork(NETWORK);
        KAFKA_CONTAINER.start();
        SCHEMA_REGISTRY_CONTAINER = new SchemaRegistryContainer("confluentinc/cp-schema-registry:5.4.4")
                .withKafka(KAFKA_CONTAINER).withNetwork(NETWORK);
        SCHEMA_REGISTRY_CONTAINER.start();
    }

    public static String getBrokers() {
        LOGGER.info("Brokers: {}", KAFKA_CONTAINER.getBootstrapServers());
        return KAFKA_CONTAINER.getBootstrapServers();
    }

    public static String getSchemaRegistryUrl() {
        LOGGER.info("Schema Registry Url: {}", SCHEMA_REGISTRY_CONTAINER.getSchemaRegistryUrl());
        return SCHEMA_REGISTRY_CONTAINER.getSchemaRegistryUrl();
    }
}
