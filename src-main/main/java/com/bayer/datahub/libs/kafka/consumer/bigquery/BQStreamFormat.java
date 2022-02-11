package com.bayer.datahub.libs.kafka.consumer.bigquery;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import static com.bayer.datahub.libs.config.PropertyNames.BQ_STREAM_FORMAT;

public enum BQStreamFormat {
    AVRO,
    JSON;

    public static BQStreamFormat parse(String option) {
        try {
            return valueOf(option.toUpperCase());
        } catch (Exception e) {
            throw new InvalidConfigurationException(BQ_STREAM_FORMAT, values());
        }
    }
}
