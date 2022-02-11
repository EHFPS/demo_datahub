package com.bayer.datahub.libs.kafka.consumer.factory;

import com.bayer.datahub.libs.config.ConfigToString;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_SCHEMA_REGISTRY_URL_PROPERTY;
import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryService.SCHEMA_REGISTRY_CONFIG;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

/**
 * {@link org.apache.kafka.clients.consumer.KafkaConsumer} is not intended for using in multi-thread code,
 * so it cannot be instantiated by Guice and used in a Quartz job.
 * Use this factory to create a KafkaConsumer instance in the thread where it will be used.
 */
class ConsumerFactoryImpl implements ConsumerFactory {
    private static final Logger log = LoggerFactory.getLogger(ConsumerFactoryImpl.class);
    private final Map<String, Object> consumerProperties;
    private final StatisticsAggregator statisticsAggregator;

    @Inject
    public ConsumerFactoryImpl(ConsumerFactoryConfigDefaults consumerFactoryConfigDefaults,
                               StatisticsAggregator statisticsAggregator, Configs configs) {
        this.statisticsAggregator = statisticsAggregator;
        Map<String, Object> consumerProperties = new HashMap<>(configs.kafkaConsumerProperties);
        consumerFactoryConfigDefaults.consumerProperties.forEach(consumerProperties::putIfAbsent);
        this.consumerProperties = consumerProperties;
    }

    @Override
    public <K, V> KafkaConsumer<K, V> newInstance() {
        log.info(ConfigToString.secureToString("Consumer properties:", consumerProperties));
        log.debug("Instantiating KafkaConsumer...");
        if (isSchemaRegistryAbsent(consumerProperties)) {
            throw new InvalidConfigurationException(KAFKA_COMMON_SCHEMA_REGISTRY_URL_PROPERTY, consumerProperties.get(SCHEMA_REGISTRY_CONFIG));
        }
        var consumer = new KafkaConsumer<>(consumerProperties);
        log.debug("KafkaConsumer is instantiated.");
        statisticsAggregator.incrementCounter("ConsumerFactory-consumer-instance-number");
        return (KafkaConsumer<K, V>) consumer;
    }

    private static boolean isSchemaRegistryAbsent(Map<String, Object> consumerProps) {
        var deserializer = (String) consumerProps.get(VALUE_DESERIALIZER_CLASS_CONFIG);
        var isAvroDeserializer = AVRO_DESERIALIZER.equalsIgnoreCase(deserializer);
        var schemaRegistryUrl = (String) consumerProps.get(SCHEMA_REGISTRY_CONFIG);
        var isSchemaRegistryUrlDefined = isBlank(schemaRegistryUrl);
        return isAvroDeserializer && isSchemaRegistryUrlDefined;
    }
}
