package com.bayer.datahub.libs.kafka.consumer.factory;

import java.util.HashMap;
import java.util.Map;

public class ConsumerFactoryConfigDefaults extends HashMap<String, Object> {
    public static final String ENABLE_AUTO_COMMIT_TRUE = "true";
    public static final String ENABLE_AUTO_COMMIT_FALSE = "false";
    public static final String AUTO_OFFSET_RESET_EARLIEST = "earliest";
    public static final String AUTO_OFFSET_RESET_LATEST = "latest";
    public final Map<String, Object> consumerProperties;

    public ConsumerFactoryConfigDefaults(Map<String, Object> consumerProperties) {
        this.consumerProperties = consumerProperties;
    }
}
