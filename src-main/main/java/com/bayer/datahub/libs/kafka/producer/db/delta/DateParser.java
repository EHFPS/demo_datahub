package com.bayer.datahub.libs.kafka.producer.db.delta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

class DateParser {
    private static final Logger log = LoggerFactory.getLogger(DateParser.class);
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSSSSS";
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final DateTimeFormatterBuilder dtfb = new DateTimeFormatterBuilder()
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.S"))
            .appendOptional(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
            .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
            .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
            .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(DATE_PATTERN);

    public static DateTimeFormatter getDateTimeFormatter() {
        return dtfb.toFormatter();
    }

    public static LocalDateTime parse(String date) {
        LocalDateTime localDateTime = null;
        if (isNotBlank(date)) {
            try {
                localDateTime = LocalDateTime.parse(date, dtfb.toFormatter());
            } catch (DateTimeParseException e) {
                try {
                    localDateTime = LocalDate.parse(date, DATE_FORMATTER).atTime(LocalTime.MIDNIGHT);
                } catch (DateTimeParseException e2) {
                    String m = format("Cannot parse date/time or date: '%s'. Possible formats: '%s' and '%s'.",
                            date, DATE_TIME_PATTERN, DATE_PATTERN);
                    throw new RuntimeException(m, e2);
                }
            }
        }
        log.debug("Date string is parsed: '{}' -> '{}'", date, localDateTime);
        return localDateTime;
    }
}
