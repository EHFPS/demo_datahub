package com.bayer.datahub.libs.kafka.consumer.cloud;

import java.nio.file.Path;

public interface CloudFileUploader {
    /**
     * For fail fast if credentials are wrong before consuming from Kafka.
     */
    void checkCredentials();

    void uploadDir(Path dir);

    void close();
}
