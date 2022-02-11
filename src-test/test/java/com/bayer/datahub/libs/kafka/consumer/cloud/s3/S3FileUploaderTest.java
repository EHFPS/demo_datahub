package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.bayer.datahub.libs.config.Configs;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;

class S3FileUploaderTest {
    private static final S3Mock s3Mock = new S3Mock();
    private final String bucketName = s3Mock.createRandomBucket();
    private final AmazonS3 s3 = s3Mock.getS3Client();

    @Test
    void uploadFile() throws IOException {
        var configs = new Configs(Map.of(
                CONSUMER_S3_RETRY_MAX_NUMBER_PROPERTY, "1",
                CONSUMER_S3_RETRY_WAIT_TIME_MS_PROPERTY, "3000",
                CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion(),
                CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY, bucketName));

        s3.createBucket(bucketName);

        var factory = s3Mock.getS3ClientFactoryMock();
        var uploader = new S3FileUploader(factory, configs);

        var dir = Files.createTempDirectory(S3FileUploaderTest.class.getSimpleName());
        var file = Files.createTempFile(dir, S3FileUploaderTest.class.getSimpleName(), ".tmp");
        var contentExp = "The file content";
        Files.write(file, contentExp.getBytes());

        uploader.uploadDir(dir);

        assertThat(readObjectContent(dir, file), equalTo(contentExp));
    }

    @Test
    void uploadFileIncludingEmpty() throws IOException {
        var configs = new Configs(Map.of(
                CONSUMER_S3_RETRY_MAX_NUMBER_PROPERTY, "1",
                CONSUMER_S3_RETRY_WAIT_TIME_MS_PROPERTY, "3000",
                CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion(),
                CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY, bucketName,
                CONSUMER_S3_UPLOAD_EMPTY_FILES_PROPERTY, "true"));

        s3.createBucket(bucketName);

        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var uploader = new S3FileUploader(s3ClientFactory, configs);

        var dir = Files.createTempDirectory(S3FileUploaderTest.class.getSimpleName());
        var fileEmpty = Files.createTempFile(dir, S3FileUploaderTest.class.getSimpleName(), ".tmp");
        var fileNotEmpty = Files.createTempFile(dir, S3FileUploaderTest.class.getSimpleName(), ".tmp");
        var contentExp = "The file content";
        Files.write(fileNotEmpty, contentExp.getBytes());

        uploader.uploadDir(dir);

        assertThat(readObjectContent(dir, fileNotEmpty), equalTo(contentExp));
        assertThat(readObjectContent(dir, fileEmpty), emptyString());
    }

    @Test
    void uploadFileSkipEmpty() throws IOException {
        var configs = new Configs(Map.of(
                CONSUMER_S3_RETRY_MAX_NUMBER_PROPERTY, "1",
                CONSUMER_S3_RETRY_WAIT_TIME_MS_PROPERTY, "3000",
                CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion(),
                CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY, bucketName,
                CONSUMER_S3_UPLOAD_EMPTY_FILES_PROPERTY, "false"));

        s3.createBucket(bucketName);

        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var uploader = new S3FileUploader(s3ClientFactory, configs);

        var dir = Files.createTempDirectory(S3FileUploaderTest.class.getSimpleName());
        var fileEmpty = Files.createTempFile(dir, S3FileUploaderTest.class.getSimpleName(), ".tmp");
        var fileNotEmpty = Files.createTempFile(dir, S3FileUploaderTest.class.getSimpleName(), ".tmp");
        var contentExp = "The file content";
        Files.write(fileNotEmpty, contentExp.getBytes());

        uploader.uploadDir(dir);

        assertThat(readObjectContent(dir, fileNotEmpty), equalTo(contentExp));
        assertThat(isObjectExist(dir, fileEmpty), equalTo(false));
    }

    private String readObjectContent(Path dir, Path file) throws IOException {
        var keyName = dir.relativize(file).toString();
        var obj = s3.getObject(bucketName, keyName);
        var is = obj.getObjectContent();
        return IOUtils.toString(is);
    }

    private boolean isObjectExist(Path dir, Path fileEmpty) {
        var keyName2 = dir.relativize(fileEmpty).toString();
        return s3.doesObjectExist(bucketName, keyName2);
    }
}