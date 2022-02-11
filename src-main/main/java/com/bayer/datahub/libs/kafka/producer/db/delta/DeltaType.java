package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_TYPE_PROPERTY;

public enum DeltaType {
    TIMESTAMP,
    DATE;

    public static DeltaType parse(String deltaTypeStr) {
        try {
            return DeltaType.valueOf(deltaTypeStr.toUpperCase());
        } catch (Exception e) {
            throw new InvalidConfigurationException(PRODUCER_DB_DELTA_TYPE_PROPERTY, deltaTypeStr, DeltaType.values(), e);
        }
    }
}
