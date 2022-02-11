package com.bayer.datahub.libs.kafka.consumer;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;

public enum ConsumerGuarantee {
    AT_LEAST_ONCE, EXACTLY_ONCE;

    public static ConsumerGuarantee parse(String guarantee, String propertyName) {
        try {
            return valueOf(guarantee.toUpperCase());
        } catch (Exception e) {
            throw new InvalidConfigurationException(propertyName, guarantee, ConsumerGuarantee.values());
        }
    }
}
