package com.bayer.datahub.libs.kafka.producer.factory;

import com.bayer.datahub.libs.config.ConfigToString;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

class ProducerFactoryImpl implements ProducerFactory {
    private static final Logger log = LoggerFactory.getLogger(ProducerFactoryImpl.class);
    private final Map<String, Object> producerProperties;
    private final StatisticsAggregator statisticsAggregator;

    @Inject
    public ProducerFactoryImpl(ProducerFactoryConfigDefaults producerFactoryConfigDefaults,
                               StatisticsAggregator statisticsAggregator, Configs configs) {
        this.statisticsAggregator = statisticsAggregator;
        var consumerProperties = new HashMap<>(configs.kafkaProducerProperties);
        producerFactoryConfigDefaults.producerProperties.forEach(consumerProperties::putIfAbsent);
        this.producerProperties = consumerProperties;
    }

    @Override
    public <K, V> KafkaProducer<K, V> newInstance() {
        log.info(ConfigToString.secureToString("Producer properties:", producerProperties));
        log.debug("Instantiating KafkaProducer...");
        var producer = new KafkaProducer<>(producerProperties);
        log.debug("KafkaProducer is instantiated.");
        statisticsAggregator.incrementCounter("ProducerFactory-producer-instance-number");
        return (KafkaProducer<K, V>) producer;
    }
}
