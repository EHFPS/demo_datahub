package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3ClientFactoryTest {
    private static final S3Mock s3Mock = new S3Mock();

    @Test
    void createS3Client() {
        var factory = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_S3.name(),
                CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY, s3Mock.getServiceEndpoint(),
                CONSUMER_S3_AWS_AUTH_TYPE_PROPERTY, S3AuthType.BASIC.name(),
                CONSUMER_S3_AWS_AUTH_BASIC_ACCESS_KEY_PROPERTY, "the-access-key",
                CONSUMER_S3_AWS_AUTH_BASIC_SECRET_KEY_PROPERTY, "the-secret-key",
                CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion()))
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .withSchemaRegistryMock()
                .build();
        var s3ClientFactory = factory.getInstance(CloudClientFactory.class);
        AmazonS3 s3Client = s3ClientFactory.createClient();
        var bucketName = s3Mock.createRandomBucket();
        s3Client.createBucket(bucketName);
        assertTrue(s3Client.doesBucketExistV2(bucketName));
        s3Client.shutdown();
    }
}