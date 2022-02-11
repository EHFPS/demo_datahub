package com.bayer.datahub.libs.kafka.producer.factory;

import org.apache.kafka.clients.producer.KafkaProducer;

public interface ProducerFactory {
    <K, V> KafkaProducer<K, V> newInstance();
}
