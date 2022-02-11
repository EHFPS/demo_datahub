package com.bayer.datahub.libs.kafka.producer.db.delta;

import org.junit.jupiter.api.Test;

import static java.time.LocalDateTime.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DateParserTest {

    @Test
    void parse() {
        assertThat(DateParser.parse("2020-10-25 17:24:37"),
                equalTo(of(2020, 10, 25, 17, 24, 37)));
        assertThat(DateParser.parse("2020-10-25"),
                equalTo(of(2020, 10, 25, 0, 0, 0)));
        assertThat(DateParser.parse(null), nullValue());
        assertThat(DateParser.parse(""), nullValue());
        assertThat(DateParser.parse(" "), nullValue());
    }

    @Test
    void parseException() {
        var e = assertThrows(RuntimeException.class, () -> DateParser.parse("25.10.2020"));
        assertThat(e.getMessage(), equalTo("Cannot parse date/time or date: '25.10.2020'. " +
                "Possible formats: 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS' and 'yyyy-MM-dd'."));
    }
}