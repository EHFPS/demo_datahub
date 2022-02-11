package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClientTypeTest {

    @Test
    void parse() {
        assertParseException(null, "Could not find a valid setting for 'client.type'. Current value: 'null'. Possible values: 'CONSUMER_FILE', 'CONSUMER_S3', 'CONSUMER_DB', 'CONSUMER_SPLUNK', 'CONSUMER_BIGQUERY', 'CONSUMER_GCS', 'CONSUMER_BQ_SLT', 'CONSUMER_ES', 'PRODUCER_SHAREDOC', 'PRODUCER_DB_PLAIN', 'PRODUCER_DB_DELTA', 'PRODUCER_CSV'. Please check your configuration.");
        assertParseException("", "Could not find a valid setting for 'client.type'. Current value: ''. Possible values: 'CONSUMER_FILE', 'CONSUMER_S3', 'CONSUMER_DB', 'CONSUMER_SPLUNK', 'CONSUMER_BIGQUERY', 'CONSUMER_GCS', 'CONSUMER_BQ_SLT', 'CONSUMER_ES', 'PRODUCER_SHAREDOC', 'PRODUCER_DB_PLAIN', 'PRODUCER_DB_DELTA', 'PRODUCER_CSV'. Please check your configuration.");
        assertParseException(" ", "Could not find a valid setting for 'client.type'. Current value: ' '. Possible values: 'CONSUMER_FILE', 'CONSUMER_S3', 'CONSUMER_DB', 'CONSUMER_SPLUNK', 'CONSUMER_BIGQUERY', 'CONSUMER_GCS', 'CONSUMER_BQ_SLT', 'CONSUMER_ES', 'PRODUCER_SHAREDOC', 'PRODUCER_DB_PLAIN', 'PRODUCER_DB_DELTA', 'PRODUCER_CSV'. Please check your configuration.");
        assertParseException("WRONG_TYPE", "Could not find a valid setting for 'client.type'. Current value: 'WRONG_TYPE'. Possible values: 'CONSUMER_FILE', 'CONSUMER_S3', 'CONSUMER_DB', 'CONSUMER_SPLUNK', 'CONSUMER_BIGQUERY', 'CONSUMER_GCS', 'CONSUMER_BQ_SLT', 'CONSUMER_ES', 'PRODUCER_SHAREDOC', 'PRODUCER_DB_PLAIN', 'PRODUCER_DB_DELTA', 'PRODUCER_CSV'. Please check your configuration.");
    }

    private static void assertParseException(String mode, String expMessage) {
        InvalidConfigurationException e = assertThrows(InvalidConfigurationException.class, () -> ClientType.parse(mode));
        assertThat(e.getMessage(), equalTo(expMessage));
    }
}