package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.ConcurrentInitializer;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import static org.mockito.Mockito.*;

public class S3Mock {
    private static final Logger log = LoggerFactory.getLogger(S3Mock.class);
    private static final String region = Regions.EU_CENTRAL_1.getName();
    private static final ConcurrentInitializer<Pair<AmazonS3, String>> initializer = new LazyInitializer<>() {
        @Override
        protected Pair<AmazonS3, String> initialize() {
            log.debug("Initializing S3Mock...");
            var api = new io.findify.s3mock.S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
            api.start();
            var region = "us-west-2";
            var serviceEndpoint = "http://localhost:8001";
            var endpoint = new AwsClientBuilder.EndpointConfiguration(serviceEndpoint, region);
            var s3 = AmazonS3ClientBuilder
                    .standard()
                    .withPathStyleAccessEnabled(true)
                    .withEndpointConfiguration(endpoint)
                    .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
                    .build();
            var s3Spy = spy(s3);
            doNothing().when(s3Spy).shutdown();//Keep S3 client alive for test assertions
            return Pair.of(s3Spy, serviceEndpoint);
        }
    };
    private static final Random random = new Random();

    public AmazonS3 getS3Client() {
        try {
            return initializer.get().getLeft();
        } catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    public CloudClientFactory getS3ClientFactoryMock() {
        return when(mock(CloudClientFactory.class).createClient()).thenReturn(getS3Client()).getMock();
    }

    public String getRegion() {
        return region;
    }

    public String getServiceEndpoint() {
        try {
            return initializer.get().getRight();
        } catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    public String createRandomBucket() {
        var bucket = "bucket-" + Math.abs(random.nextInt());
        getS3Client().createBucket(bucket);
        return bucket;
    }

}
