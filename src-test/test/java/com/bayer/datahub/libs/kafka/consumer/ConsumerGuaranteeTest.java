package com.bayer.datahub.libs.kafka.consumer;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_BASE_GUARANTEE_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumerGuaranteeTest {

    private static void assertParseException(String mode, String expMessage) {
        var e = assertThrows(InvalidConfigurationException.class,
                () -> ConsumerGuarantee.parse(mode, CONSUMER_BASE_GUARANTEE_PROPERTY));
        assertThat(e.getMessage(), equalTo(expMessage));
    }

    @Test
    void parse() {
        assertParseException(null, "Could not find a valid setting for 'consumer.base.guarantee'. Current value: 'null'. Possible values: 'AT_LEAST_ONCE', 'EXACTLY_ONCE'. Please check your configuration.");
        assertParseException("", "Could not find a valid setting for 'consumer.base.guarantee'. Current value: ''. Possible values: 'AT_LEAST_ONCE', 'EXACTLY_ONCE'. Please check your configuration.");
        assertParseException(" ", "Could not find a valid setting for 'consumer.base.guarantee'. Current value: ' '. Possible values: 'AT_LEAST_ONCE', 'EXACTLY_ONCE'. Please check your configuration.");
        assertParseException("WRONG_GUARANTEE", "Could not find a valid setting for 'consumer.base.guarantee'. Current value: 'WRONG_GUARANTEE'. Possible values: 'AT_LEAST_ONCE', 'EXACTLY_ONCE'. Please check your configuration.");
    }
}