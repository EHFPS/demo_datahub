package com.bayer.datahub.libs.kafka.producer.factory;

import java.util.HashMap;
import java.util.Map;

public class ProducerFactoryConfigDefaults extends HashMap<String, Object> {
    public final Map<String, Object> producerProperties;

    public ProducerFactoryConfigDefaults(Map<String, Object> producerProperties) {
        this.producerProperties = producerProperties;
    }
}
