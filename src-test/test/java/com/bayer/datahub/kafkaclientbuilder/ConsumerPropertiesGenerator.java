package com.bayer.datahub.kafkaclientbuilder;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.Random;

public class ConsumerPropertiesGenerator {
    private static final Random random = new Random();

    public static Properties genPropertiesWithGroupId() {
        var properties = new Properties();

        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-" + random.nextInt(Integer.MAX_VALUE));

        return properties;
    }
}
