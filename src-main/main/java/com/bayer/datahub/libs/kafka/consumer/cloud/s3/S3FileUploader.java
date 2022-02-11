package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.Retry;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudFileUploader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

class S3FileUploader implements CloudFileUploader {
    private static final Logger log = LoggerFactory.getLogger(S3FileUploader.class);
    private final CloudClientFactory cloudClientFactory;
    private final int retryMaxNumber;
    private final int retryWaitTimeMs;
    private final String bucketName;
    private final boolean uploadEmptyFiles;
    private AmazonS3 s3Client;

    @Inject
    public S3FileUploader(CloudClientFactory cloudClientFactory, Configs configs) {
        this.cloudClientFactory = cloudClientFactory;
        retryMaxNumber = configs.consumerS3RetryMaxNumber;
        retryWaitTimeMs = configs.consumerS3RetryWaitTimeMs;
        bucketName = configs.consumerS3AwsBucketName;
        uploadEmptyFiles = configs.consumerS3UploadEmptyFiles;
    }

    private static long countEmptyFiles(List<File> files) {
        return files.stream().filter(file -> file.length() == 0).count();
    }

    @Override
    public void checkCredentials() {
        log.debug("Checking S3 credentials...");
        AmazonS3 client = cloudClientFactory.createClient();
        client.shutdown();
        log.debug("S3 credentials are correct.");
    }

    @Override
    public void uploadDir(Path dir) {
        var retrySend = new Retry(retryMaxNumber, retryWaitTimeMs);
        while (retrySend.shouldRetry()) {
            try {
                tryUploadDir(dir);
                break;
            } catch (Exception e) {
                log.warn(e.getMessage(), e);
                retrySend.retryCatch();
            }
        }
    }

    private void tryUploadDir(Path dir) {
        try {
            log.debug("Start uploading directory '{}' to S3", dir);
            if (!Files.isDirectory(dir)) {
                throw new IllegalArgumentException("Directory expected, but file is found: " + dir);
            }
            s3Client = cloudClientFactory.createClient();
            var files = Files.walk(dir)
                    .filter(path -> !Files.isDirectory(path))
                    .map(Path::toFile)
                    .collect(Collectors.toList());
            uploadFiles(dir.toFile(), files);
            log.debug("Finish uploading directory '{}' to S3", dir);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadFiles(File baseDir, List<File> files) {
        try {
            if (!uploadEmptyFiles) {
                log.debug("Skip empty files: {}", countEmptyFiles(files));
                files = files.stream().filter(file -> file.length() > 0).collect(Collectors.toList());
            }
            var fileLength = files.stream().mapToLong(File::length).sum();
            log.info("Uploading files to S3 bucket: total file number={}, empty file number={}, total file size in bytes={}, bucket='{}'",
                    files.size(), countEmptyFiles(files), fileLength, bucketName);
            var tm = TransferManagerBuilder.standard().withS3Client(s3Client).build();
            var upload = tm.uploadFileList(bucketName, null, baseDir, files);
            log.debug("Waiting for completion of S3 uploading...");
            upload.waitForCompletion();
            log.debug("S3 uploading is completed.");
            tm.shutdownNow(false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (s3Client != null) {
            s3Client.shutdown();
        }
    }
}
