package com.bayer.datahub.libs.kafka.chunk;

import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SyncAsyncProducerTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();
    private final KafkaProducer<String, String> kafkaProducer = spy(KAFKA_CLIENT_HELPER.createStringProducer());

    @Test
    void sendAsync() {
        var recordNumber = 1000;
        var expRecords = IntStream.range(0, recordNumber)
                .mapToObj(number -> new ProducerRecord<String, String>(topic, "Value #" + number))
                .collect(Collectors.toList());
        try (var producer = new SyncAsyncProducer<>(kafkaProducer, mock(StatisticsAggregator.class))) {
            expRecords.forEach(producer::sendAsync);
            verify(kafkaProducer, times(recordNumber)).send(any());
        }
        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(recordNumber));
        var actRecordValues = actRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        var expRecordValues = expRecords.stream().map(ProducerRecord::value).toArray(String[]::new);
        assertThat(actRecordValues, containsInAnyOrder(expRecordValues));
    }

    @Test
    void sendSync() {
        var recordNumber = 1000;
        var expRecords = IntStream.range(0, recordNumber)
                .mapToObj(number -> new ProducerRecord<String, String>(topic, "Value #" + number))
                .collect(Collectors.toList());
        try (var producer = new SyncAsyncProducer<>(kafkaProducer, mock(StatisticsAggregator.class))) {
            producer.sendSync(expRecords);
            verify(kafkaProducer, times(recordNumber)).send(any());
        }
        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(recordNumber));
        var actRecordValues = actRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        var expRecordValues = expRecords.stream().map(ProducerRecord::value).toArray(String[]::new);
        assertThat(actRecordValues, contains(expRecordValues));
    }

    @Test
    void sendSyncAsync() {
        var asyncRecordNumber1 = 1000;
        var syncRecordNumber2 = 1500;
        var asyncRecordNumber3 = 2000;
        var totalRecords = asyncRecordNumber1 + syncRecordNumber2 + asyncRecordNumber3;
        var expAsyncRecords1 = IntStream.range(0, asyncRecordNumber1)
                .mapToObj(number -> new ProducerRecord<String, String>(topic, "Async-1_Value-" + number))
                .collect(Collectors.toList());
        var expSyncRecords2 = IntStream.range(0, syncRecordNumber2)
                .mapToObj(number -> new ProducerRecord<String, String>(topic, "Sync-2_Value-" + number))
                .collect(Collectors.toList());
        var expAsyncRecords3 = IntStream.range(0, asyncRecordNumber3)
                .mapToObj(number -> new ProducerRecord<String, String>(topic, "Async-3_Value-" + number))
                .collect(Collectors.toList());
        try (var producer = new SyncAsyncProducer<>(kafkaProducer, mock(StatisticsAggregator.class))) {
            expAsyncRecords1.forEach(producer::sendAsync);
            producer.sendSync(expSyncRecords2);
            expAsyncRecords3.forEach(producer::sendAsync);
            verify(kafkaProducer, times(totalRecords)).send(any());
        }

        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(totalRecords));
        var actRecordValues = actRecords.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        var expAsyncRecordValues1 = expAsyncRecords1.stream().map(ProducerRecord::value).toArray(String[]::new);
        var expSyncRecordValues2 = expSyncRecords2.stream().map(ProducerRecord::value).toArray(String[]::new);
        var expAsyncRecordValues3 = expAsyncRecords3.stream().map(ProducerRecord::value).toArray(String[]::new);

        assertThat(actRecordValues, hasItems(expAsyncRecordValues1));
        assertThat(actRecordValues, containsInRelativeOrder(expSyncRecordValues2));
        assertThat(actRecordValues, hasItems(expAsyncRecordValues3));

        var actAsyncRecordValues1 = actRecordValues.subList(0, asyncRecordNumber1);
        assertThat(actAsyncRecordValues1, containsInAnyOrder(expAsyncRecordValues1));
        var actSyncRecordValues2 = actRecordValues.subList(asyncRecordNumber1, asyncRecordNumber1 + syncRecordNumber2);
        assertThat(actSyncRecordValues2, contains(expSyncRecordValues2));
        var actAsyncRecordValues3 = actRecordValues.subList(asyncRecordNumber1 + syncRecordNumber2,
                asyncRecordNumber1 + syncRecordNumber2 + asyncRecordNumber3);
        assertThat(actAsyncRecordValues3, containsInAnyOrder(expAsyncRecordValues3));
    }
}