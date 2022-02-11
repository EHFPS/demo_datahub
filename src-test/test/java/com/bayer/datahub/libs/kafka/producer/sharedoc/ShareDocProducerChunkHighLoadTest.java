package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.util.IOUtils;
import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.WebMockServerBaseTest;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3AuthType;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3Mock;
import com.bayer.datahub.libs.services.json.JsonService;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

class ShareDocProducerChunkHighLoadTest extends WebMockServerBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(AbstractKafkaBaseTest.getBrokers(), AbstractKafkaBaseTest.getSchemaRegistryUrl());

    private static final S3Mock s3Mock = new S3Mock();
    private static final String JSESSIONID_COOKIE = format("JSESSIONID=%s; Path=/cara-rest/; Secure; HttpOnly",
            "38117334AB3CA361D4756A3ABC0BF2AE");
    private static final DocumentGenerator generator = new DocumentGenerator();
    private final ShareDocJsonService shareDocJsonService = new ShareDocJsonService(new JsonService());
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();

    @Test
    void manyRecords() throws IOException {
        var docNumber1 = 100;
        var docNumber2 = 50;
        var chunkSize = 1000;

        var zeroDocRatio = 0.1;
        var bigDocRatio = 0.3;

        var docs = runProducer(List.of(), docNumber1, chunkSize, zeroDocRatio, bigDocRatio);
        runProducer(docs, docNumber2, chunkSize, zeroDocRatio, bigDocRatio);
    }

    private List<DocumentGenerator.Doc> runProducer(List<DocumentGenerator.Doc> prevRunDocs, int docNumber,
                                                    int chunkSize, double zeroDocRatio, double bigDocRatio) throws IOException {
        var newDocs = Stream.generate(() -> generator.generateDocSize(zeroDocRatio, bigDocRatio, chunkSize))
                .map(generator::createNewDoc)
                .limit(docNumber).collect(toList());

        var docs = new ArrayList<>(prevRunDocs);
        docs.addAll(newDocs);

        var prevZeroDocNum = (int) prevRunDocs.stream().filter(doc -> doc.content.length == 0).count();
        var zeroDocNum = (int) docs.stream().filter(doc -> doc.content.length == 0).count();
        System.out.println("zeroDocNum=" + zeroDocNum);

        var smallDocNum = (int) docs.stream().map(doc -> doc.content.length)
                .filter(length -> length > 0 && length <= chunkSize).count();
        System.out.println("smallDocNum=" + smallDocNum);

        var bigDocNum = (int) docs.stream().filter(doc -> doc.content.length > chunkSize).count();
        System.out.println("bigDocNum=" + bigDocNum);

        var metadataRowList = docs.stream()
                .map(doc -> doc.metadataRow)
                .collect(Collectors.toUnmodifiableList());
        var metadata = generator.mergeMetadataToSingleJson(metadataRowList);
        var metadataJson = shareDocJsonService.serializeMetadata(metadata);

        var prevChunkNumber = calcChunkNumber(prevRunDocs, chunkSize) + prevZeroDocNum;
        var chunkNumber = calcChunkNumber(docs, chunkSize) + zeroDocNum + prevChunkNumber;
        System.out.println("chunkNumber=" + chunkNumber);

        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", JSESSIONID_COOKIE);
        addPathResponse("services/tools/getqueryresults/the_repo", metadataJson);
        docs.forEach(doc -> addPathResponse("/services/content/export/the_repo/" + doc.id, doc.content));

        var configs = createConfigs(chunkSize);

        var producer = FactoryBuilder.newBuilder(configs)
                .withByteArrayProducerFactory()
                .withByteArrayConsumerFactory()
                .withSchemaRegistryMock()
                .build().getKafkaClient();
        producer.init();
        producer.run();
        producer.stop();

        //Assert topic
        var records = KAFKA_CLIENT_HELPER.consumeAllBinaryRecordsFromBeginning(topic);
        assertThat(records, hasSize(chunkNumber));

        //Assert S3
        var headerName = "filename";

        var bucketName = s3Mock.createRandomBucket();
        s3Mock.getS3Client().createBucket(bucketName);
        var outputDir = createTempDirectory(ShareDocProducerChunkHighLoadTest.class.getSimpleName());

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
        var expDocNumber = docNumber + prevRunDocs.size();
        assertThat(objects, hasSize(expDocNumber));
        var keyToObjectMap = objects.stream().collect(toMap(S3ObjectSummary::getKey, object -> object));
        for (DocumentGenerator.Doc doc : docs) {
            var key = "files-the_repo/" + doc.id + "-T103130-8.pdf";
            var summary = keyToObjectMap.get(key);
            assertThat(summary.getSize(), equalTo((long) doc.content.length));
            assertS3Object(key, doc.content, bucketName, s3Client, keyToObjectMap.get(key));
        }

        return docs;
    }

    private int calcChunkNumber(List<DocumentGenerator.Doc> docs, int chunkSize) {
        return docs.stream()
                .mapToInt(doc -> (int) Math.ceil((double) doc.content.length / chunkSize))
                .sum();
    }

    @Test
    void chunkNumberTest() {
        BiFunction<Integer, Integer, Integer> chunkNumber = (length, chunkSize) -> (int) Math.ceil((double) length / chunkSize);
        assertThat(chunkNumber.apply(1000, 100), equalTo(10));
        assertThat(chunkNumber.apply(999, 1000), equalTo(1));
        assertThat(chunkNumber.apply(1000, 1000), equalTo(1));
        assertThat(chunkNumber.apply(1001, 1000), equalTo(2));
        assertThat(chunkNumber.apply(0, 1000), equalTo(0));
        assertThat(chunkNumber.apply(1_024_000_000, 50_000), equalTo(20480));
    }

    private Map<String, String> createConfigs(int chunkSize) {
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
                entry(CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_S3.name()),
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

    private void assertS3Object(String fileName, byte[] expContent, String bucketName, AmazonS3 s3Client,
                                S3ObjectSummary summary) throws IOException {
        var object = s3Client.getObject(bucketName, summary.getKey());
        assertThat(fileName, equalTo(object.getKey()));
        var actContent = IOUtils.toByteArray(object.getObjectContent());
        assertThat(actContent, equalTo(expContent));
    }
}