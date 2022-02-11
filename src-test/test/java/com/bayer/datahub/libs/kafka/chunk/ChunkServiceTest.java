package com.bayer.datahub.libs.kafka.chunk;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_PROPERTY;
import static com.bayer.datahub.libs.kafka.chunk.ChunkService.*;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ChunkServiceTest {
    private static final String topic = "the_topic";
    private static final String key = "the_key";

    @Test
    void splitTwoRecords() {
        var value = "abc".repeat(1000).getBytes();
        var record = new ProducerRecord<>(topic, key, value);
        var chunkTotal = 2;
        var chunkSize = value.length / chunkTotal;
        var chunkService = createChunkService(chunkSize);
        var recordList = chunkService.split(record);
        assertThat(recordList, hasSize(chunkTotal));
        assertRecord(recordList.get(0), record, chunkSize, 0, chunkTotal);
        assertRecord(recordList.get(1), record, chunkSize, 1, chunkTotal);
    }

    @Test
    void splitTreeRecords() {
        var value = "abc".repeat(1000).getBytes();
        var record = new ProducerRecord<>(topic, key, value);
        var chunkTotal = 3;
        var chunkSize = value.length / chunkTotal;
        var chunkService = createChunkService(chunkSize);
        var recordList = chunkService.split(record);
        assertThat(recordList, hasSize(chunkTotal));
        assertRecord(recordList.get(0), record, chunkSize, 0, chunkTotal);
        assertRecord(recordList.get(1), record, chunkSize, 1, chunkTotal);
        assertRecord(recordList.get(2), record, chunkSize, 2, chunkTotal);
    }

    @Test
    void notSplitSmallRecord() {
        var value = "abc".repeat(1000).getBytes();
        var record = new ProducerRecord<>(topic, key, value);
        var chunkSize = value.length + 1;
        var chunkService = createChunkService(chunkSize);
        var recordList = chunkService.split(record);
        assertThat(recordList, hasSize(1));
        assertThat(recordList, contains(record));
    }

    @Test
    void chunkDisabled() {
        var value = "abc".repeat(1000).getBytes();
        var chunkSize = 0;
        var chunkService = createChunkService(chunkSize);
        var record = new ProducerRecord<>(topic, key, value);
        var recordList = chunkService.split(record);
        assertThat(recordList, hasSize(1));
        assertThat(recordList, contains(record));
    }

    @Test
    void nullValue() {
        var chunkService = createChunkService(1000);
        var record = new ProducerRecord<>(topic, key, (byte[]) null);
        var recordList = chunkService.split(record);
        assertThat(recordList, hasSize(1));
        assertThat(recordList, contains(record));
    }

    @Test
    void emptyValue() {
        var chunkService = createChunkService(1000);
        var record = new ProducerRecord<>(topic, key, new byte[0]);
        var recordList = chunkService.split(record);
        assertThat(recordList, hasSize(1));
        assertThat(recordList, contains(record));
    }

    @Test
    void valueIsNotByteArray() {
        var chunkService = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_SHAREDOC.name()))
                .build().getInstance(ChunkService.class);
        var record = new ProducerRecord<>(topic, key, "abc");
        var e = assertThrows(ChunkException.class, () -> chunkService.split(record));
        assertThat(e.getMessage(), equalTo("ChunkSplitter supports only byte array values. Actual value type: String"));
    }

    private static ChunkService<String, byte[]> createChunkService(int chunkSize) {
        return FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_PROPERTY, String.valueOf(chunkSize)))
                .build().getInstance(ChunkService.class);
    }

    private static void assertRecord(ProducerRecord<String, byte[]> actRecord, ProducerRecord<String, byte[]> expRecord,
                                     int chunkSize, int chunkNum, int chunkTotal) {
        assertThat(actRecord.topic(), equalTo(expRecord.topic()));
        assertThat(actRecord.key(), equalTo(expRecord.key()));

        var from = chunkSize * chunkNum;
        var to = chunkSize * (chunkNum + 1);
        assertThat(actRecord.value(), equalTo(Arrays.copyOfRange(expRecord.value(), from, to)));

        var actHeaders = asList(actRecord.headers().toArray());
        assertThat(actHeaders, hasItems(expRecord.headers().toArray()));
        var chunkUuidHeader = new String(actRecord.headers().lastHeader(CHUNK_UUID_HEADER).value());
        assertThat(chunkUuidHeader, not(emptyString()));
        var chunkNumHeader = byteArrayToInt(actRecord.headers().lastHeader(CHUNK_NUM_HEADER).value());
        assertThat(chunkNumHeader, equalTo(chunkNum));
        var chunkTotalHeader = byteArrayToInt(actRecord.headers().lastHeader(CHUNK_TOTAL_HEADER).value());
        assertThat(chunkTotalHeader, equalTo(chunkTotal));
    }
}