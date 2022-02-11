package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;

import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;

public enum ClientType {
    CONSUMER_FILE,
    CONSUMER_S3,
    CONSUMER_DB,
    CONSUMER_SPLUNK,
    CONSUMER_BIGQUERY,
    CONSUMER_GCS,
    CONSUMER_BQ_SLT,
    CONSUMER_ES,
    PRODUCER_SHAREDOC,
    PRODUCER_DB_PLAIN,
    PRODUCER_DB_DELTA,
    PRODUCER_CSV;

    public static ClientType parse(String clientTypeStr) {
        try {
            return valueOf(clientTypeStr.toUpperCase());
        } catch (Exception e) {
            throw new InvalidConfigurationException(CLIENT_TYPE_PROPERTY, clientTypeStr, values(), e);
        }
    }
}
