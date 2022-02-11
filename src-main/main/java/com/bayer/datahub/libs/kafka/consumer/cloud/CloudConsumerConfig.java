package com.bayer.datahub.libs.kafka.consumer.cloud;

import com.bayer.datahub.libs.services.fileio.FileFormat;

public class CloudConsumerConfig {
    public final String topic;
    public final String objectName;
    public final FileFormat formatType;
    public final int retryMaxNumber;
    public final int retryWaitTimeMs;
    public final String outputDir;
    public final String formatBinaryHeaderName;
    public final char formatCsvValueDelimiter;
    public final String formatCsvRecordDelimiter;
    public final String formatCsvCharset;

    public CloudConsumerConfig(String topic, String objectName, FileFormat formatType, int retryMaxNumber,
                               int retryWaitTimeMs, String outputDir, String formatBinaryHeaderName,
                               char formatCsvValueDelimiter, String formatCsvRecordDelimiter, String formatCsvCharset) {
        this.topic = topic;
        this.objectName = objectName;
        this.formatType = formatType;
        this.retryMaxNumber = retryMaxNumber;
        this.retryWaitTimeMs = retryWaitTimeMs;
        this.outputDir = outputDir;
        this.formatBinaryHeaderName = formatBinaryHeaderName;
        this.formatCsvValueDelimiter = formatCsvValueDelimiter;
        this.formatCsvRecordDelimiter = formatCsvRecordDelimiter;
        this.formatCsvCharset = formatCsvCharset;
    }
}
