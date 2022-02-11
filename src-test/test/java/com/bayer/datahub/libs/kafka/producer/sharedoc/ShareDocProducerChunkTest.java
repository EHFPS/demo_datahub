package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.WebMockServerBaseTest;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3AuthType;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3Mock;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;

import static com.bayer.datahub.ResourceHelper.resourceToString;
import static com.bayer.datahub.kafkaclientbuilder.ClientType.CONSUMER_S3;
import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_SHAREDOC;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.EXACTLY_ONCE;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.Authenticator.LOGIN_ENDPOINT_PATH;
import static com.bayer.datahub.libs.services.fileio.FileFormat.BINARY;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Map.entry;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ShareDocProducerChunkTest extends WebMockServerBaseTest {
    private static final S3Mock s3Mock = new S3Mock();
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(AbstractKafkaBaseTest.getBrokers(), AbstractKafkaBaseTest.getSchemaRegistryUrl());
    private static final String JSESSIONID_COOKIE = format("JSESSIONID=%s; Path=/cara-rest/; Secure; HttpOnly",
            "38117334AB3CA361D4756A3ABC0BF2AE");
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();

    @Test
    void oneBigRecord() throws IOException {
        var metadataBody = resourceToString(ShareDocProducerChunkTest.class, "ShareDocProducerChunkTest_oneBigRecord.json");
        var content1Small = "the small body 1";
        var content2Big = "the big big big big big body 2";
        var content3Small = "the small body 3";

        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", JSESSIONID_COOKIE);
        addPathResponse("services/tools/getqueryresults/the_repo", metadataBody);
        addPathResponse("/services/content/export/the_repo/090192f2802548b4", content1Small);
        addPathResponse("/services/content/export/the_repo/090192f28025489d", content2Big);
        addPathResponse("/services/content/export/the_repo/090192f2802534d7", content3Small);

        var chunkSize = content1Small.length() + 1;
        var producer = FactoryBuilder.newBuilder(createConfigMap(chunkSize))
                .withByteArrayProducerFactory()
                .withByteArrayConsumerFactory()
                .build().getKafkaClient();
        producer.init();
        producer.run();
        producer.stop();

        var records = KAFKA_CLIENT_HELPER.consumeAllBinaryRecordsFromBeginning(topic);
        assertThat(records, hasSize(4));
        var valueList = records.stream().map(ConsumerRecord::value).map(String::new).map(String::trim).collect(toList());
        var bigChunk1 = content2Big.substring(0, chunkSize);
        var bigChunk2 = content2Big.substring(chunkSize);
        assertThat(valueList, containsInAnyOrder(content1Small, content3Small, bigChunk1, bigChunk2));


        var headerName = "filename";

        var bucketName = s3Mock.createRandomBucket();
        s3Mock.getS3Client().createBucket(bucketName);
        var outputDir = createTempDirectory(ShareDocProducerChunkTest.class.getSimpleName());

        var consumerConfigMap = createConsumerConfigMap(BINARY.name(), outputDir.toString(),
                headerName, bucketName, EXACTLY_ONCE);
        var s3Consumer = FactoryBuilder.newBuilder(consumerConfigMap)
                .withByteArrayConsumerFactory()
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .withSchemaRegistryMock()
                .build().getKafkaClient();
        s3Consumer.init();
        s3Consumer.run();
        s3Consumer.stop();

        var s3Client = s3Mock.getS3Client();
        var objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
        assertThat(objects, hasSize(3));
        var keyToObjectMap = objects.stream().collect(toMap(S3ObjectSummary::getKey, object -> object));
        var key1 = "files-the_repo/090192f2802534d7-T103563-8.pdf";
        var key2 = "files-the_repo/090192f28025489d-T103112-8.pdf";
        var key3 = "files-the_repo/090192f2802548b4-T103130-8.pdf";
        assertStringS3Object(key3, content1Small, bucketName, s3Client, keyToObjectMap.get(key3));
        assertStringS3Object(key1, content3Small, bucketName, s3Client, keyToObjectMap.get(key1));
        assertStringS3Object(key2, content2Big, bucketName, s3Client, keyToObjectMap.get(key2));
    }

    @Test
    void twoBigRecords() throws IOException {
        var metadataBody = resourceToString(ShareDocProducerChunkTest.class, "ShareDocProducerChunkTest_twoBigRecords.json");
        var content1Small = "the small body 1";
        var content2Big = "the big2 big big big big body 2";
        var content3Small = "the small body 3";
        var content4Big = "the big4 big big big big body 4";
        var content5Small = "the small body 5";
        var content6Small = "the small body 6";

        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", JSESSIONID_COOKIE);
        addPathResponse("services/tools/getqueryresults/the_repo", metadataBody);
        addPathResponse("/services/content/export/the_repo/090192f2802548b4", content1Small);
        addPathResponse("/services/content/export/the_repo/090192f28025489d", content2Big);
        addPathResponse("/services/content/export/the_repo/090192f2802534d7", content3Small);
        addPathResponse("/services/content/export/the_repo/090192f2802534bc", content4Big);
        addPathResponse("/services/content/export/the_repo/090192f280253493", content5Small);
        addPathResponse("/services/content/export/the_repo/090192f280253475", content6Small);

        var chunkSize = content1Small.length() + 1;

        var producer = FactoryBuilder.newBuilder(createConfigMap(chunkSize))
                .withByteArrayProducerFactory()
                .withByteArrayConsumerFactory()
                .build().getKafkaClient();
        producer.init();
        producer.run();
        producer.stop();

        var records = KAFKA_CLIENT_HELPER.consumeAllBinaryRecordsFromBeginning(topic);
        assertThat(records, hasSize(8));
        var valueList = records.stream().map(ConsumerRecord::value).map(String::new).map(String::trim).collect(toList());
        var bigChunk2A = content2Big.substring(0, chunkSize).trim();
        var bigChunk2B = content2Big.substring(chunkSize).trim();
        var bigChunk4A = content4Big.substring(0, chunkSize).trim();
        var bigChunk4B = content4Big.substring(chunkSize).trim();
        assertThat(valueList, containsInAnyOrder(content1Small, content3Small, bigChunk2A, bigChunk2B,
                bigChunk4A, bigChunk4B, content5Small, content6Small));


        var headerName = "filename";

        var bucketName = s3Mock.createRandomBucket();
        s3Mock.getS3Client().createBucket(bucketName);
        var outputDir = createTempDirectory(ShareDocProducerChunkTest.class.getSimpleName());

        var consumerConfigMap = createConsumerConfigMap(BINARY.name(), outputDir.toString(),
                headerName, bucketName, EXACTLY_ONCE);
        var s3Consumer = FactoryBuilder.newBuilder(consumerConfigMap)
                .withByteArrayConsumerFactory()
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .withSchemaRegistryMock()
                .build().getKafkaClient();
        s3Consumer.init();
        s3Consumer.run();
        s3Consumer.stop();

        var s3Client = s3Mock.getS3Client();
        var objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
        assertThat(objects, hasSize(6));
        var keyToObjectMap = objects.stream().collect(toMap(S3ObjectSummary::getKey, object -> object));
        var key1 = "files-the_repo/090192f2802534d7-T103563-8.pdf";
        var key2 = "files-the_repo/090192f28025489d-T103112-8.pdf";
        var key3 = "files-the_repo/090192f2802548b4-T103130-8.pdf";
        var key4 = "files-the_repo/090192f280253475-T103925-0.pdf";
        var key5 = "files-the_repo/090192f280253493-T103290-5.pdf";
        var key6 = "files-the_repo/090192f2802534bc-T103529-0.pdf";
        assertStringS3Object(key1, content3Small, bucketName, s3Client, keyToObjectMap.get(key1));
        assertStringS3Object(key2, content2Big, bucketName, s3Client, keyToObjectMap.get(key2));
        assertStringS3Object(key3, content1Small, bucketName, s3Client, keyToObjectMap.get(key3));
        assertStringS3Object(key4, content6Small, bucketName, s3Client, keyToObjectMap.get(key4));
        assertStringS3Object(key5, content5Small, bucketName, s3Client, keyToObjectMap.get(key5));
        assertStringS3Object(key6, content4Big, bucketName, s3Client, keyToObjectMap.get(key6));
    }

    private Map<String, String> createConfigMap(int chunkSize) {
        return Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                PRODUCER_SHAREDOC_MODE_PROPERTY, Mode.CONTENT.name(),
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, "the_repo",
                PRODUCER_SHAREDOC_USERNAME_PROPERTY, "the_username",
                PRODUCER_SHAREDOC_PASSWORD_PROPERTY, "the_property",
                PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_PROPERTY, String.valueOf(chunkSize));
    }

    private Map<String, String> createConsumerConfigMap(String fileFormat, String outputDir, String headerName,
                                                        String bucketName, ConsumerGuarantee guarantee) {
        return Map.ofEntries(
                entry(CLIENT_TYPE_PROPERTY, CONSUMER_S3.name()),
                entry("kafka.consumer.group.id", "the_group"),
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

    private void assertStringS3Object(String fileName, String expContentStr, String bucketName, AmazonS3 s3Client,
                                      S3ObjectSummary summary) throws IOException {
        var object = s3Client.getObject(bucketName, summary.getKey());
        assertThat(fileName, equalTo(object.getKey()));
        var actContentStr = IOUtils.toString(object.getObjectContent());
        assertThat(actContentStr.trim(), equalTo(expContentStr.trim()));
    }
}