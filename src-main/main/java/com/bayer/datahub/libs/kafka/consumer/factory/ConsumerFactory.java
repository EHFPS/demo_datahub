package com.bayer.datahub.libs.kafka.consumer.factory;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public interface ConsumerFactory {
    <K, V> KafkaConsumer<K, V> newInstance();
}
