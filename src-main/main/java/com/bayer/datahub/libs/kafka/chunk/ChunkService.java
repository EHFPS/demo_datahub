package com.bayer.datahub.libs.kafka.chunk;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class ChunkService<K, V> {
    private static final Logger log = LoggerFactory.getLogger(ChunkService.class);
    public static final String CHUNK_UUID_HEADER = "chunk_uuid";
    static final String CHUNK_NUM_HEADER = "chunk_num";
    static final String CHUNK_TOTAL_HEADER = "chunk_total";
    private final StatisticsAggregator statisticsAggregator;
    private final int chunkSizeBytes;
    private final boolean chunkEnabled;
    private final Queue<ConsumerRecord<K, V>> mergedRecordQueue = new LinkedList<>();
    private final Queue<ConsumerRecord<K, V>> chunkedRecordQueue = new LinkedList<>();

    @Inject
    public ChunkService(StatisticsAggregator statisticsAggregator, Configs configs) {
        this.statisticsAggregator = statisticsAggregator;
        chunkSizeBytes = configs.producerSharedocChunkSizeBytes;
        chunkEnabled = chunkSizeBytes > 0;
        log.info("Chunk size is '{}'.", chunkSizeBytes);
        log.info("ChunkSplitter is {}.", chunkEnabled ? "enabled" : "disabled");
    }

    public List<ProducerRecord<K, V>> split(ProducerRecord<K, V> record) {
        if (!chunkEnabled) {
            return List.of(record);
        }
        if (record.value() == null) {
            return List.of(record);
        }
        if (!(record.value() instanceof byte[])) {
            throw new ChunkException("ChunkSplitter supports only byte array values. " +
                    "Actual value type: " + record.value().getClass().getSimpleName());
        }
        var valueBytes = (byte[]) record.value();
        if (valueBytes.length > chunkSizeBytes) {
            var firstPosition = 0;
            var newRecordList = new ArrayList<ProducerRecord<K, V>>();
            var uuid = UUID.randomUUID().toString();
            var chunkNum = 0;
            var chunkTotal = (int) Math.ceil((double) valueBytes.length / chunkSizeBytes);
            while (firstPosition < valueBytes.length) {
                var lastPosition = firstPosition + chunkSizeBytes;
                if (lastPosition > valueBytes.length) {
                    lastPosition = valueBytes.length;
                }
                var newValueBytes = Arrays.copyOfRange(valueBytes, firstPosition, lastPosition);
                var headers = new ArrayList<>(Arrays.asList(record.headers().toArray()));
                headers.add(new RecordHeader(CHUNK_UUID_HEADER, uuid.getBytes()));
                headers.add(new RecordHeader(CHUNK_NUM_HEADER, intToByteArray(chunkNum)));
                headers.add(new RecordHeader(CHUNK_TOTAL_HEADER, intToByteArray(chunkTotal)));
                var newRecord = new ProducerRecord<>(record.topic(), record.partition(), record.key(), (V) newValueBytes, headers);
                newRecordList.add(newRecord);
                firstPosition = lastPosition;
                chunkNum++;
            }
            log.debug("Record is split into chunks: chunkUuid={}, chunkNum={}, chunkSizeBytes={}, originalSizeBytes={}",
                    uuid, newRecordList.size(), chunkSizeBytes, valueBytes.length);
            checkProducerRecordChunks(newRecordList);
            statisticsAggregator.incrementCounter("ChunkService-records-split-to-chunks");
            statisticsAggregator.incrementCounter("ChunkService-split-chunk-number", newRecordList.size());
            return newRecordList;
        }
        statisticsAggregator.incrementCounter("ChunkService-records-not-split-to-chunks");
        return List.of(record);
    }

    public List<ConsumerRecord<K, V>> accumulate(List<ConsumerRecord<K, V>> consumerRecordList) {
        for (ConsumerRecord<K, V> record : consumerRecordList) {
            if (isChunkRecord(record)) {
                if (isLastChunkRecord(record)) {
                    chunkedRecordQueue.add(record);
                    var mergedRecord = merge(new ArrayList<>(chunkedRecordQueue));
                    mergedRecordQueue.add(mergedRecord);
                    chunkedRecordQueue.clear();
                } else {
                    chunkedRecordQueue.add(record);
                }
                statisticsAggregator.incrementCounter("ChunkService-merged-chunk-number");
            } else {
                mergedRecordQueue.add(record);
                statisticsAggregator.incrementCounter("ChunkService-record-not-need-to-merge-number");
            }
        }
        var result = new ArrayList<>(mergedRecordQueue);
        mergedRecordQueue.clear();
        return result;
    }

    private boolean isChunkRecord(ConsumerRecord<K, V> record) {
        if (record == null) {
            return false;
        }
        return record.headers().lastHeader(CHUNK_UUID_HEADER) != null;
    }

    private boolean isChunkRecord(ProducerRecord<K, V> record) {
        if (record == null) {
            return false;
        }
        return record.headers().lastHeader(CHUNK_UUID_HEADER) != null;
    }

    private boolean isLastChunkRecord(ConsumerRecord<K, V> record) {
        if (!isChunkRecord(record)) {
            return false;
        }
        var chunkNum = record.headers().lastHeader(CHUNK_NUM_HEADER);
        if (chunkNum == null) {
            throw new ChunkException(CHUNK_NUM_HEADER + " header absents.");
        }
        var chunkTotal = record.headers().lastHeader(CHUNK_TOTAL_HEADER);
        if (chunkTotal == null) {
            throw new ChunkException(CHUNK_TOTAL_HEADER + " header absents.");
        }
        return byteArrayToInt(chunkNum.value()) == (byteArrayToInt(chunkTotal.value()) - 1);
    }

    private boolean isLastChunkRecord(ProducerRecord<K, V> record) {
        if (!isChunkRecord(record)) {
            return false;
        }
        var chunkNum = record.headers().lastHeader(CHUNK_NUM_HEADER);
        if (chunkNum == null) {
            throw new ChunkException(CHUNK_NUM_HEADER + " header absents.");
        }
        var chunkTotal = record.headers().lastHeader(CHUNK_TOTAL_HEADER);
        if (chunkTotal == null) {
            throw new ChunkException(CHUNK_TOTAL_HEADER + " header absents.");
        }
        return byteArrayToInt(chunkNum.value()) == (byteArrayToInt(chunkTotal.value()) - 1);
    }

    private ConsumerRecord<K, V> merge(List<ConsumerRecord<K, V>> records) {
        if (records.isEmpty()) {
            throw new ChunkException("Empty record list.");
        }
        if (records.size() == 1) {
            return records.get(0);
        }
        checkConsumerRecordChunks(records);
        var totalSize = records.stream()
                .map(record -> ((byte[]) record.value()).length)
                .mapToInt(Integer::intValue)
                .sum();
        var mergedArray = new byte[totalSize];
        var destPos = 0;
        for (ConsumerRecord<K, V> record : records) {
            var value = (byte[]) record.value();
            System.arraycopy(value, 0, mergedArray, destPos, value.length);
            destPos = destPos + value.length;
        }
        var firstRecord = records.get(0);
        var lastRecord = records.get(records.size() - 1);
        var headerList = new ArrayList<>(Arrays.asList(firstRecord.headers().toArray()));
        headerList.removeIf(header -> CHUNK_UUID_HEADER.equals(header.key()));
        headerList.removeIf(header -> CHUNK_NUM_HEADER.equals(header.key()));
        headerList.removeIf(header -> CHUNK_TOTAL_HEADER.equals(header.key()));
        Headers headers = new RecordHeaders(headerList);
        var serializedValueSize = records.stream().mapToInt(ConsumerRecord::serializedValueSize).sum();
        var uuid = new String(records.get(0).headers().lastHeader(CHUNK_UUID_HEADER).value());
        log.debug("Chunks are merged into a record: chunkUuid={}, chunkNum={}, recordSizeBytes={}",
                uuid, records.size(), totalSize);
        return new ConsumerRecord<>(firstRecord.topic(), firstRecord.partition(), lastRecord.offset(),
                firstRecord.timestamp(), firstRecord.timestampType(), firstRecord.checksum(),
                firstRecord.serializedKeySize(), serializedValueSize,
                firstRecord.key(), (V) mergedArray, headers);
    }

    private void checkProducerRecordChunks(List<ProducerRecord<K, V>> records) {
        var uniqueUuids = records.stream().map(ProducerRecord::headers)
                .map(headers -> headers.lastHeader(CHUNK_UUID_HEADER).value())
                .map(String::new)
                .distinct()
                .collect(toList());
        if (uniqueUuids.size() > 1) {
            throw new ChunkException("Chunks have different UUID: " + uniqueUuids);
        }

        var uniquePartitions = records.stream().map(ProducerRecord::partition).distinct().collect(toList());
        if (uniquePartitions.size() > 1) {
            throw new ChunkException("Chunks have different partitions: " + uniquePartitions);
        }

        var uniqueTopics = records.stream().map(ProducerRecord::topic).distinct().collect(toList());
        if (uniqueTopics.size() > 1) {
            throw new ChunkException("Chunks have different topics: " + uniqueTopics);
        }

        var uniqueKeys = records.stream().map(ProducerRecord::key).distinct().collect(toList());
        if (uniqueKeys.size() > 1) {
            throw new ChunkException("Chunks have different keys: " + uniqueKeys);
        }

        var chunkNumbers = records.stream().map(ProducerRecord::headers)
                .map(headers -> headers.lastHeader(CHUNK_NUM_HEADER))
                .map(header -> byteArrayToInt(header.value()))
                .collect(toList());
        var expChunkNumbers = IntStream.range(0, records.size()).boxed().collect(toList());
        if (!chunkNumbers.equals(expChunkNumbers)) {
            throw new ChunkException("Wrong chunk numbers: " + chunkNumbers);
        }

        if (!isLastChunkRecord(records.get(records.size() - 1))) {
            throw new ChunkException("Last chunk is not the last chunk");
        }
    }

    private void checkConsumerRecordChunks(List<ConsumerRecord<K, V>> records) {
        var uniqueUuids = records.stream().map(ConsumerRecord::headers)
                .map(headers -> headers.lastHeader(CHUNK_UUID_HEADER).value())
                .map(String::new)
                .distinct()
                .collect(toList());
        if (uniqueUuids.size() > 1) {
            throw new ChunkException("Chunks have different UUID: " + uniqueUuids);
        }

        var uniquePartitions = records.stream().map(ConsumerRecord::partition).distinct().collect(toList());
        if (uniquePartitions.size() > 1) {
            throw new ChunkException("Chunks have different partitions: " + uniquePartitions);
        }

        var uniqueTopics = records.stream().map(ConsumerRecord::topic).distinct().collect(toList());
        if (uniqueTopics.size() > 1) {
            throw new ChunkException("Chunks have different topics: " + uniqueTopics);
        }

        var uniqueKeys = records.stream().map(ConsumerRecord::key).distinct().collect(toList());
        if (uniqueKeys.size() > 1) {
            throw new ChunkException("Chunks have different keys: " + uniqueKeys);
        }

        var chunkNumbers = records.stream().map(ConsumerRecord::headers)
                .map(headers -> headers.lastHeader(CHUNK_NUM_HEADER))
                .map(header -> byteArrayToInt(header.value()))
                .collect(toList());
        var expChunkNumbers = IntStream.range(0, records.size()).boxed().collect(toList());
        if (!chunkNumbers.equals(expChunkNumbers)) {
            throw new ChunkException("Wrong chunk numbers: " + chunkNumbers);
        }

        if (!isLastChunkRecord(records.get(records.size() - 1))) {
            throw new ChunkException("Last chunk is not the last chunk");
        }
    }

    static int byteArrayToInt(byte[] bytes) {
        return new BigInteger(bytes).intValue();
    }

    static byte[] intToByteArray(int integer) {
        return BigInteger.valueOf(integer).toByteArray();
    }
}
