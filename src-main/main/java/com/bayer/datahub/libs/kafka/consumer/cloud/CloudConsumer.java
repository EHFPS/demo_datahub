package com.bayer.datahub.libs.kafka.consumer.cloud;

import com.bayer.datahub.libs.kafka.chunk.ChunkService;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.kafka.consumer.file.FileConsumer;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import javax.inject.Inject;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * {@link CloudConsumer} consumes messages to a temp local file
 * and uploads it to a cloud storage (S3, Azure Blob Storage, Google Cloud Storage).
 */
public class CloudConsumer extends FileConsumer {
    private final CloudFileUploader cloudFileUploader;
    private final Path outputDir;
    private final FileFormat fileFormat;

    @Inject
    public CloudConsumer(RecordConsumer recordConsumer, CloudFileUploader cloudFileUploader,
                         ChunkService<String, byte[]> chunkService, CloudConsumerConfig ccConfig,
                         RecordService recordService) {
        super(recordConsumer, ccConfig.topic, ccConfig.objectName,
                ccConfig.formatType, ccConfig.retryMaxNumber, ccConfig.retryWaitTimeMs,
                ccConfig.outputDir, ccConfig.formatBinaryHeaderName,
                ccConfig.formatCsvValueDelimiter, ccConfig.formatCsvRecordDelimiter,
                ccConfig.formatCsvCharset, chunkService, recordService);
        this.cloudFileUploader = cloudFileUploader;
        outputDir = Paths.get(ccConfig.outputDir);
        fileFormat = ccConfig.formatType;
    }

    @Override
    public void init() {
        super.init();
        cloudFileUploader.checkCredentials();
    }

    @Override
    protected void processRecordsExactlyOnce(ConsumerRecords<?, ?> records) {
        super.processRecordsExactlyOnce(records);
        if (fileFormat == FileFormat.BINARY) {
            upload();
        }
    }

    @Override
    protected void commitEnd() {
        upload();
        cloudFileUploader.close();
    }

    private void upload() {
        try {
            cloudFileUploader.uploadDir(outputDir);
            cleanDestination();
            commit();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}