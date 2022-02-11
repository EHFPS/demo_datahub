package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.AT_LEAST_ONCE;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.EXACTLY_ONCE;
import static com.bayer.datahub.libs.services.fileio.FileFormat.BINARY;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Map.entry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.*;

class S3ConsumerBinaryTest extends AbstractKafkaBaseTest {
    private static final S3Mock s3Mock = new S3Mock();
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());

    @Test
    @Timeout(60)
    void exactlyOnce() throws IOException, ExecutionException, InterruptedException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();

        var headerName = "filename";

        var fileName1 = "out-1.bin";
        var expContentStr1 = "the content 1";

        var fileName2 = "out-2.bin";
        var expContentStr2 = "the content 2";

        var fileName3 = "out-3.bin";
        var expContentStr3 = "the content 3";

        var producer = KAFKA_CLIENT_HELPER.createBinaryProducer();
        produceBinaryRecord(producer, topic, headerName, fileName1, expContentStr1);
        produceBinaryRecord(producer, topic, headerName, fileName2, expContentStr2);
        produceBinaryRecord(producer, topic, headerName, fileName3, expContentStr3);

        var bucketName = s3Mock.createRandomBucket();
        s3Mock.getS3Client().createBucket(bucketName);
        var outputDir = createTempDirectory(S3ConsumerBinaryTest.class.getSimpleName());

        var binaryConsumerPropertiesOverride = new Properties();
        binaryConsumerPropertiesOverride.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

        var config = createConfigMap(BINARY.name(), outputDir.toString(), topic, headerName, bucketName,
                EXACTLY_ONCE);
        var configs = new Configs(config);//TODO remove
        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var s3FileUploader = spy(new S3FileUploader(s3ClientFactory, configs));
        var s3Consumer = FactoryBuilder.newBuilder(config)
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .override(S3FileUploader.class, s3FileUploader)
                .withByteArrayConsumerFactory(binaryConsumerPropertiesOverride)
                .withSchemaRegistryMock()
                .build().getKafkaClient();
        s3Consumer.init();
        s3Consumer.run();
        s3Consumer.stop();

        verify(s3FileUploader, times(3)).uploadDir(outputDir);

        var s3Client = s3Mock.getS3Client();
        var objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
        assertThat(objects, hasSize(3));

        assertStringS3Object(fileName1, expContentStr1, bucketName, s3Client, objects.get(0));
        assertStringS3Object(fileName2, expContentStr2, bucketName, s3Client, objects.get(1));
        assertStringS3Object(fileName3, expContentStr3, bucketName, s3Client, objects.get(2));
    }

    @Test
    @Timeout(60)
    void atLeastOnce() throws IOException, ExecutionException, InterruptedException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();

        var headerName = "filename";

        var fileName1 = "out-1.bin";
        var expContentStr1 = "the content 1";

        var fileName2 = "out-2.bin";
        var expContentStr2 = "the content 2";

        var fileName3 = "out-3.bin";
        var expContentStr3 = "the content 3";

        var producer = KAFKA_CLIENT_HELPER.createBinaryProducer();
        produceBinaryRecord(producer, topic, headerName, fileName1, expContentStr1);
        produceBinaryRecord(producer, topic, headerName, fileName2, expContentStr2);
        produceBinaryRecord(producer, topic, headerName, fileName3, expContentStr3);

        var bucketName = s3Mock.createRandomBucket();
        s3Mock.getS3Client().createBucket(bucketName);
        var outputDir = createTempDirectory(S3ConsumerBinaryTest.class.getSimpleName());

        var binaryConsumerPropertiesOverride = new Properties();
        binaryConsumerPropertiesOverride.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

        var config = createConfigMap(BINARY.name(), outputDir.toString(), topic, headerName, bucketName,
                AT_LEAST_ONCE);
        var configs = new Configs(config);//TODO remove
        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var s3FileUploader = spy(new S3FileUploader(s3ClientFactory, configs));
        var s3Consumer = FactoryBuilder.newBuilder(config)
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .override(S3FileUploader.class, s3FileUploader)
                .withByteArrayConsumerFactory(binaryConsumerPropertiesOverride)
                .withSchemaRegistryMock()
                .build().getKafkaClient();
        s3Consumer.init();
        s3Consumer.run();
        s3Consumer.stop();

        verify(s3FileUploader, times(1)).uploadDir(outputDir);

        var s3Client = s3Mock.getS3Client();
        var objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
        assertThat(objects, hasSize(3));

        assertStringS3Object(fileName1, expContentStr1, bucketName, s3Client, objects.get(0));
        assertStringS3Object(fileName2, expContentStr2, bucketName, s3Client, objects.get(1));
        assertStringS3Object(fileName3, expContentStr3, bucketName, s3Client, objects.get(2));
    }

    @Test
    void binaryFormatNoFilenameHeader() throws ExecutionException, InterruptedException, IOException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();

        var headerNameProducer = "filename1";
        var headerNameConsumer = "filename2";

        var producer = KAFKA_CLIENT_HELPER.createBinaryProducer();
        produceBinaryRecord(producer, topic, headerNameProducer, "out-1.bin", "the content 1");

        var outputDir = createTempDirectory(S3ConsumerBinaryTest.class.getSimpleName());
        var config = createConfigMap(BINARY.name(), outputDir.toString(),
                topic, headerNameConsumer, s3Mock.createRandomBucket(), EXACTLY_ONCE);
        var configs = new Configs(config);//TODO remove
        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var s3FileUploader = spy(new S3FileUploader(s3ClientFactory, configs));
        var consumer = FactoryBuilder.newBuilder(config)
                .withByteArrayConsumerFactory()
                .override(S3FileUploader.class, s3FileUploader)
                .withSchemaRegistryMock()
                .build().getKafkaClient();
        var e = Assertions.assertThrows(IllegalArgumentException.class, () -> {
            consumer.init();
            consumer.run();
        });
        consumer.stop();
        assertThat(e.getMessage(), equalTo("Kafka record header 'filename2' is null"));
    }

    private void assertStringS3Object(String fileName, String expContentStr, String bucketName, AmazonS3 s3Client,
                                      S3ObjectSummary summary) throws IOException {
        var object = s3Client.getObject(bucketName, summary.getKey());
        assertThat(fileName, equalTo(object.getKey()));
        var actContentStr = IOUtils.toString(object.getObjectContent());
        assertThat(actContentStr, equalTo(expContentStr));
    }

    private void produceBinaryRecord(KafkaProducer<String, byte[]> producer, String topic, String headerName,
                                     String fileName, String valueStr) throws InterruptedException, ExecutionException {
        var recordKey = "the-key";
        var expContent = valueStr.getBytes();
        var fileNameHeader = new RecordHeader(headerName, fileName.getBytes());
        var headers = new RecordHeaders(Collections.singletonList(fileNameHeader));
        var record = new ProducerRecord<>(topic, 0, recordKey, expContent, headers);
        producer.send(record).get();
    }

    private Map<String, String> createConfigMap(String fileFormat, String outputDir, String topic, String headerName,
                                                String bucketName, ConsumerGuarantee guarantee) {
        return Map.ofEntries(
                entry(CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_S3.name()),
                entry(KAFKA_COMMON_TOPIC_PROPERTY, topic),
                entry(CONSUMER_BASE_GUARANTEE_PROPERTY, guarantee.name()),
                entry(CONSUMER_S3_FORMAT_TYPE_PROPERTY, fileFormat),
                entry(CONSUMER_S3_FORMAT_CSV_S3_KEY_NAME_PROPERTY, ""),
                entry(CONSUMER_S3_OUTPUT_DIR_PROPERTY, outputDir),
                entry(CONSUMER_S3_FORMAT_BINARY_HEADER_NAME_PROPERTY, headerName),
                entry(CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY, bucketName),
                entry(CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion()),
                entry(CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY, s3Mock.getServiceEndpoint()),
                entry(CONSUMER_S3_AWS_AUTH_TYPE_PROPERTY, S3AuthType.AUTO.name()));
    }
}