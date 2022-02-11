package com.bayer.datahub.libs.kafka.consumer.file;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.Consumer;
import com.bayer.datahub.libs.kafka.chunk.ChunkService;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.kafka.consumer.Retry;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import com.bayer.datahub.libs.services.fileio.FileWriter;
import com.google.common.collect.Lists;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * FileConsumer consumes messages to a local file.
 */
public class FileConsumer implements Consumer {
    private static final Logger log = LoggerFactory.getLogger(FileConsumer.class);
    private final Path filePath;
    private final FileFormat fileFormat;
    private final int maxRetries;
    private final int retryWaitTimeMs;
    private final String outputDir;
    private final String headerName;
    private final char csvValueDelimiter;
    private final String csvRecordDelimiter;
    private final String csvCharset;
    private final ChunkService<String, byte[]> chunkService;
    private final String topic;
    private final RecordService recordService;
    protected RecordConsumer recordConsumer;
    private FileWriter fileWriter;

    @Inject
    public FileConsumer(RecordConsumer recordConsumer, ChunkService<String, byte[]> chunkService,
                        RecordService recordService, Configs configs) {
        this(recordConsumer, configs.kafkaCommonTopic, configs.consumerFileFilePath, configs.consumerFileFileFormat,
                configs.consumerFileRetryMaxNumber, configs.consumerFileRetryWaitTimeMs,
                configs.consumerFileBinaryOutputDir, configs.consumerFileBinaryHeaderName,
                configs.consumerFileFormatCsvValueDelimiter, configs.consumerFileFormatCsvRecordDelimiter,
                configs.consumerFileFormatCsvCharset, chunkService, recordService);
    }

    public FileConsumer(RecordConsumer recordConsumer, String topic, String filePath,
                        FileFormat fileFormat, int maxRetries,
                        int retryWaitTimeMs, String outputDir, String headerName, char csvValueDelimiter,
                        String csvRecordDelimiter, String csvCharset, ChunkService<String, byte[]> chunkService,
                        RecordService recordService) {
        this.topic = topic;
        this.filePath = Paths.get(outputDir, filePath);
        this.fileFormat = fileFormat;
        this.maxRetries = maxRetries;
        this.retryWaitTimeMs = retryWaitTimeMs;
        this.outputDir = outputDir;
        this.headerName = headerName;
        this.csvValueDelimiter = csvValueDelimiter;
        this.csvRecordDelimiter = csvRecordDelimiter;
        this.csvCharset = csvCharset;
        this.chunkService = chunkService;
        this.recordConsumer = recordConsumer;
        this.recordService = recordService;
    }

    @Override
    public void init() {
        recordConsumer.init();
        try {
            fileWriter = FileWriter.getInstance(fileFormat, outputDir, headerName, csvValueDelimiter,
                    csvRecordDelimiter, csvCharset);
            fileWriter.open(filePath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void run() {
        recordConsumer.run(this::processRecordsAtLeastOnce, this::processRecordsExactlyOnce);
        closeWriter();
        commitEnd();
    }

    @Override
    public void stop() {
        recordConsumer.stop();
    }

    protected void processRecordsExactlyOnce(ConsumerRecords<?, ?> records) {
        processRecordsAtLeastOnce(records);
    }

    protected void processRecordsAtLeastOnce(ConsumerRecords<?, ?> records) {
        if (!records.isEmpty()) {
            if (fileFormat != FileFormat.BINARY) {
                List<GenericRecord> genericRecords = recordService.consumerRecordsToGenericRecords((ConsumerRecords<?, GenericRecord>) records);
                log.info("Writing {} records to file {}...", genericRecords.size(), fileWriter.getPath());
                fileWriter.writeGenericRecords(genericRecords);
                log.info("{} records are saved to file {}", genericRecords.size(), fileWriter.getPath());
            } else {
                Iterable recordsIterable = records.records(topic);
                var consumerRecords = Lists.newArrayList(recordsIterable);
                var mergedRecords = chunkService.accumulate(consumerRecords);
                fileWriter.writeConsumerRecords(mergedRecords);
            }
        }
    }

    protected void cleanDestination() {
        fileWriter.cleanDestination();
    }

    protected void closeWriter() {
        try {
            fileWriter.close();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    protected void commit() {
        var retry = new Retry(maxRetries, retryWaitTimeMs);
        while (retry.shouldRetry()) {
            try {
                log.debug("Committing offset...");
                recordConsumer.commit();
                break;
            } catch (WakeupException e) {
                log.debug("Cannot commit because of WakeupException");
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                retry.retryCatch();
            }
        }
    }

    protected void commitEnd() {
        commit();
    }
}