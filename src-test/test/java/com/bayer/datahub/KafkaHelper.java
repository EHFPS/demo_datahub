package com.bayer.datahub;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.hamcrest.Matcher;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class KafkaHelper {
    public static String consumeRecordsToString(String topic, ConsumerRecords<?, ?> consumerRecords) {
        return StreamSupport.stream(consumerRecords.records(topic).spliterator(), false)
                .map(record -> record.key() + "-" + record.value())
                .collect(Collectors.joining());
    }

    public static void assertStringConsumerRecord(ConsumerRecord<String, String> record, Matcher<? super String> keyMatcher,
                                                  Matcher<? super String> valueMatcher, String expHeaderName, byte[] expHeaderValue) {
        assertThat(record.key(), keyMatcher);
        assertThat(record.value(), valueMatcher);
        var headers = record.headers();
        var filenameHeader = headers.lastHeader(expHeaderName);
        assertNotNull(filenameHeader);
        assertThat(filenameHeader.key(), equalTo(expHeaderName));
        assertThat(filenameHeader.value(), equalTo(expHeaderValue));
    }

    public static void assertBinaryConsumerRecord(ConsumerRecord<String, byte[]> record, Matcher<? super String> keyMatcher,
                                                  Matcher<? super byte[]> valueMatcher, String expHeaderName, byte[] expHeaderValue) {
        assertThat(record.key(), keyMatcher);
        assertThat(record.value(), valueMatcher);
        var headers = record.headers();
        var filenameHeader = headers.lastHeader(expHeaderName);
        assertNotNull(filenameHeader);
        assertThat(filenameHeader.key(), equalTo(expHeaderName));
        assertThat(filenameHeader.value(), equalTo(expHeaderValue));
    }
}
