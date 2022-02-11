package com.bayer.datahub.libs.kafka.consumer.bigquery;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;

import static com.bayer.datahub.libs.config.PropertyNames.GCS_LOAD_OPTION;

public enum GcsLoadOption {
    JSON,
    AVRO,
    NDJSON,
    NONE;

    public static GcsLoadOption parse(String loadOp) {
        try {
            return valueOf(loadOp.toUpperCase());
        } catch (Exception e) {
            throw new InvalidConfigurationException(GCS_LOAD_OPTION, values());
        }
    }
}
