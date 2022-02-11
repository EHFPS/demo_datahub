package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ModeTest {

    @Test
    void parse() {
        assertThat(Mode.parse("mEtAdAtA"), equalTo(Mode.METADATA));
        assertThat(Mode.parse("cOnTeNt"), equalTo(Mode.CONTENT));
    }

    @Test
    void parseExceptions() {
        assertParseException(null, "Could not find a valid setting for 'producer.sharedoc.mode'. Current value: 'null'. Possible values: 'METADATA', 'CONTENT'. Please check your configuration.");
        assertParseException("", "Could not find a valid setting for 'producer.sharedoc.mode'. Current value: ''. Possible values: 'METADATA', 'CONTENT'. Please check your configuration.");
        assertParseException(" ", "Could not find a valid setting for 'producer.sharedoc.mode'. Current value: ' '. Possible values: 'METADATA', 'CONTENT'. Please check your configuration.");
        assertParseException("WRONG_MODE", "Could not find a valid setting for 'producer.sharedoc.mode'. Current value: 'WRONG_MODE'. Possible values: 'METADATA', 'CONTENT'. Please check your configuration.");
    }

    private static void assertParseException(String mode, String expMessage) {
        var e = assertThrows(InvalidConfigurationException.class, () -> Mode.parse(mode));
        assertThat(e.getMessage(), equalTo(expMessage));
    }

}