package com.bayer.datahub.libs.services.fileio;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static java.nio.file.Files.createTempDirectory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;

class BinaryFileWriterTest {
    private static final String headerName = "filename";

    @Test
    void writeConsumerRecords() throws IOException {
        Path outputDir = createTempDirectory(BinaryFileWriterTest.class.getSimpleName());
        FileWriter writer = new BinaryFileWriter(outputDir.toString(), "filename");

        String topic = "the-topic";
        int partition = 0;

        String fileName1 = "abc-1.txt";
        String contentStr1 = "the file content #1";
        ConsumerRecord<String, byte[]> record1 = createBinaryConsumerRecord(fileName1, contentStr1, topic, partition);

        String fileName2 = "abc-2.txt";
        String contentStr2 = "the file content #2";
        ConsumerRecord<String, byte[]> record2 = createBinaryConsumerRecord(fileName2, contentStr2, topic, partition);

        List<ConsumerRecord<String, byte[]>> recordList = Arrays.asList(record1, record2);
        ConsumerRecords<String, byte[]> records = createBinaryConsumerRecords(topic, partition, recordList);

        writer.open(null);
        writer.writeConsumerRecords(records);
        writer.close();

        assertThat(readFileContent(outputDir, fileName1), equalTo(contentStr1));
        assertThat(readFileContent(outputDir, fileName2), equalTo(contentStr2));
    }

    @Test
    void cleanOutputDirOnOpen() throws IOException {
        Path outputDir = createTempDirectory(BinaryFileWriterTest.class.getSimpleName());
        Path existingFile = outputDir.resolve("existing_file.txt");
        Files.write(existingFile, "content".getBytes());
        assertTrue(Files.exists(existingFile));

        FileWriter writer = new BinaryFileWriter(outputDir.toString(), "filename");
        writer.open(existingFile);

        assertTrue(Files.exists(outputDir));
        assertFalse(Files.exists(existingFile));
    }

    @Test
    void writeConsumerRecordsNotBinaryValueException() throws IOException {
        Path outputDir = createTempDirectory(BinaryFileWriterTest.class.getSimpleName());
        FileWriter writer = new BinaryFileWriter(outputDir.toString(), headerName);

        String topic = "the-topic";
        int partition = 0;

        String fileName = "abc-1.txt";
        Header fileNameHeader = new RecordHeader(headerName, fileName.getBytes());
        Headers headers = new RecordHeaders(Collections.singletonList(fileNameHeader));
        long timestamp = System.currentTimeMillis();
        String recordKey = "the-key";
        String recordValue = "the file content #1";
        ConsumerRecord<String, String> record = new ConsumerRecord<>(topic, partition, 0, timestamp,
                TimestampType.CREATE_TIME, 0L, recordKey.getBytes().length, recordValue.getBytes().length,
                recordKey, recordValue, headers);

        List<ConsumerRecord<String, String>> recordList = Collections.singletonList(record);
        TopicPartition tp = new TopicPartition(topic, partition);
        Map<TopicPartition, List<ConsumerRecord<String, String>>> map = new HashMap<>();
        map.put(tp, recordList);
        ConsumerRecords<String, String> records = new ConsumerRecords<>(map);

        Throwable e = assertThrows(IllegalArgumentException.class, () -> {
            writer.open(null);
            writer.writeConsumerRecords(records);
        });
        assertThat(e.getMessage(), equalTo("byte[] record value is expected, but actual is java.lang.String"));
    }

    private ConsumerRecords<String, byte[]> createBinaryConsumerRecords(String topic, int partition,
                                                                        List<ConsumerRecord<String, byte[]>> recordList) {
        TopicPartition tp = new TopicPartition(topic, partition);
        Map<TopicPartition, List<ConsumerRecord<String, byte[]>>> map = new HashMap<>();
        map.put(tp, recordList);
        return new ConsumerRecords<>(map);
    }

    private ConsumerRecord<String, byte[]> createBinaryConsumerRecord(String fileName, String contentStr, String topic,
                                                                      int partition) {
        Header fileNameHeader = new RecordHeader(headerName, fileName.getBytes());
        Headers headers = new RecordHeaders(Collections.singletonList(fileNameHeader));
        long timestamp = System.currentTimeMillis();
        String recordKey = "the-key";
        byte[] recordValue = contentStr.getBytes();
        return new ConsumerRecord<>(topic, partition, 0, timestamp,
                TimestampType.CREATE_TIME, 0L, recordKey.getBytes().length, recordValue.length,
                recordKey, recordValue, headers);
    }

    private static String readFileContent(Path dir, String fileName) throws IOException {
        Path file = Paths.get(dir.toString(), fileName);
        return FileUtils.readFileToString(file.toFile());
    }

}