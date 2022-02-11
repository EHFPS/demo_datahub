package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.amazonaws.util.IOUtils;
import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.kafkaclientbuilder.ConsumerPropertiesGenerator;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.services.common.statistics.Statistics;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.StatisticsAssert.assertStatistics;
import static com.bayer.datahub.libs.StatisticsAssert.sortedMapOf;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.AT_LEAST_ONCE;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.EXACTLY_ONCE;
import static com.bayer.datahub.libs.services.common.statistics.Statistics.Group.groupOf;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.CONSUMER_METRICS_GROUP;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.COUNTER_GROUP;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.*;

class S3ConsumerCsvTest extends AbstractKafkaBaseTest {
    private static final S3Mock s3Mock = new S3Mock();
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private static final Properties OVERRIDE_CONSUMER_CONFIG = ConsumerPropertiesGenerator.genPropertiesWithGroupId();
    private static final String FIELD_NAME_A = "columnA";
    private static final String FIELD_NAME_B = "columnB";
    private static final Schema SCHEMA = SchemaBuilder.record("the_record").namespace("the_namespace")
            .fields()
            .name(FIELD_NAME_A).type().stringType().noDefault()
            .name(FIELD_NAME_B).type().stringType().noDefault()
            .endRecord();
    private static final String FILE_PATH = "subdir/csvFormat.csv";
    private final String bucketName = s3Mock.createRandomBucket();
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();


    private static ProducerRecord<String, Object> createRecord(String topic, String fieldAValue, String fieldBValue) {
        GenericRecord genericRecord = new GenericData.Record(SCHEMA);
        genericRecord.put(FIELD_NAME_A, fieldAValue);
        genericRecord.put(FIELD_NAME_B, fieldBValue);
        return new ProducerRecord<>(topic, genericRecord);
    }

    @Test
    void exactlyOnce() throws InterruptedException, ExecutionException, IOException {
        var records = asList(
                createRecord(topic, "Desk1", "2019-06-10 00:00:00"),
                createRecord(topic, "Desk2", "2020-06-10 00:00:00"),
                createRecord(topic, "Desk3", "2021-06-10 00:00:00")
        );
        produce(records);
        var factory = runConsumer(EXACTLY_ONCE);
        assertS3("columnA,columnB\nDesk1,2019-06-10 00:00:00\nDesk2,2020-06-10 00:00:00\nDesk3,2021-06-10 00:00:00\n");
        assertFinalStatistics(factory, 3);
    }

    private void assertFinalStatistics(FactoryBuilder.Factory factory, int consumedRecords) {
        var statistics = factory.getInstance(StatisticsAggregator.class).getStatistics();
        assertStatistics(
                Statistics.Type.FINAL,
                statistics,
                "Final statistics for " + topic,
                " ",
                null,
                Statistics.Status.SUCCESS,
                List.of(groupOf(COUNTER_GROUP, sortedMapOf("Consumed-records", consumedRecords)),
                        groupOf(CONSUMER_METRICS_GROUP, sortedMapOf())));
    }

    @Test
    void exactlyOnceSingleRecord() throws InterruptedException, ExecutionException, IOException {
        var records = singletonList(
                createRecord(topic, "Desk1", "2019-06-10 00:00:00")
        );
        produce(records);
        var factory = runConsumer(EXACTLY_ONCE);
        assertS3("columnA,columnB\nDesk1,2019-06-10 00:00:00\n");
        assertFinalStatistics(factory, 1);
    }

    @Test
    void atLeastOnce() throws IOException, ExecutionException, InterruptedException {
        var records = asList(
                createRecord(topic, "Desk1", "2019-06-10 00:00:00"),
                createRecord(topic, "Desk2", "2020-06-10 00:00:00"),
                createRecord(topic, "Desk3", "2021-06-10 00:00:00")
        );
        produce(records);
        var factory = runConsumer(AT_LEAST_ONCE);
        assertS3("columnA,columnB\nDesk1,2019-06-10 00:00:00\nDesk2,2020-06-10 00:00:00\nDesk3,2021-06-10 00:00:00\n");
        assertFinalStatistics(factory, 3);
    }

    @Test
    void atLeastOnceEmptyTopic() throws IOException, ExecutionException, InterruptedException {
        List<ProducerRecord<String, Object>> records = emptyList();
        produce(records);
        var factory = runConsumer(AT_LEAST_ONCE);
        assertS3("");
        assertFinalStatistics(factory, 0);
    }

    @Test
    void atLeastOnceEmptyTopicWithBeginning() throws IOException, ExecutionException, InterruptedException {
        var records = asList(
                createRecord(topic, "Desk1", "2019-06-10 00:00:00"),
                createRecord(topic, "Desk2", "2020-06-10 00:00:00"),
                createRecord(topic, "Desk3", "2021-06-10 00:00:00")
        );
        produce(records);
        KAFKA_CLIENT_HELPER.deleteRecords(topic, records.size());
        var factory = runConsumer(AT_LEAST_ONCE);
        assertS3("");
        assertFinalStatistics(factory, 0);
    }

    @Test
    void atLeastOnceEmptyTopicWithCommittedOffsetLessThanBeginning() throws IOException, ExecutionException, InterruptedException {
        produce(asList(
                createRecord(topic, "Desk0", "2010-06-10 00:00:00"),
                createRecord(topic, "Desk1", "2011-06-10 00:00:00"),
                createRecord(topic, "Desk2", "2012-06-10 00:00:00")
        ));
        runConsumer(AT_LEAST_ONCE);
        assertS3("columnA,columnB\nDesk0,2010-06-10 00:00:00\nDesk1,2011-06-10 00:00:00\nDesk2,2012-06-10 00:00:00\n");
        produce(asList(
                createRecord(topic, "Desk3", "2013-06-10 00:00:00"),
                createRecord(topic, "Desk4", "2014-06-10 00:00:00"),
                createRecord(topic, "Desk5", "2015-06-10 00:00:00"),
                createRecord(topic, "Desk6", "2016-06-10 00:00:00")
        ));
        KAFKA_CLIENT_HELPER.deleteRecords(topic, 5);
        var factory = runConsumer(AT_LEAST_ONCE);
        assertS3("columnA,columnB\nDesk5,2015-06-10 00:00:00\nDesk6,2016-06-10 00:00:00\n");
        assertFinalStatistics(factory, 2);
    }

    @Test
    void atLeastOnceNoNewMessagesInTopic() throws IOException, ExecutionException, InterruptedException {
        produce(asList(
                createRecord(topic, "Desk0", "2010-06-10 00:00:00"),
                createRecord(topic, "Desk1", "2011-06-10 00:00:00"),
                createRecord(topic, "Desk2", "2012-06-10 00:00:00")
        ));
        runConsumer(AT_LEAST_ONCE);
        assertS3("columnA,columnB\nDesk0,2010-06-10 00:00:00\nDesk1,2011-06-10 00:00:00\nDesk2,2012-06-10 00:00:00\n");
        var factory = runConsumer(AT_LEAST_ONCE);
        assertS3("");
        assertFinalStatistics(factory, 0);
    }

    private void produce(List<ProducerRecord<String, Object>> records)
            throws ExecutionException, InterruptedException {
        var producer = KAFKA_CLIENT_HELPER.createAvroProducer();

        for (var record : records) {
            producer.send(record).get();
        }
    }

    private FactoryBuilder.Factory runConsumer(ConsumerGuarantee guarantee) throws IOException {
        OVERRIDE_CONSUMER_CONFIG.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);
        s3Mock.getS3Client().createBucket(bucketName);

        var outputDir = createTempDirectory(S3ConsumerCsvTest.class.getSimpleName());
        var config = createConfigMap(outputDir.toString(), guarantee);
        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var configs = new Configs(config);//TODO remove
        var s3FileUploader = spy(new S3FileUploader(s3ClientFactory, configs));
        var factory = FactoryBuilder.newBuilder(config)
                .withAvroConsumerFactory(OVERRIDE_CONSUMER_CONFIG)
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .override(S3FileUploader.class, s3FileUploader)
                .withSchemaRegistryMock()
                .build();
        var s3Consumer = factory.getKafkaClient();
        s3Consumer.init();
        s3Consumer.run();
        s3Consumer.stop();

        verify(s3FileUploader, times(1)).uploadDir(outputDir);
        return factory;
    }

    private void assertS3(String expContent) throws IOException {
        var s3Client = s3Mock.getS3Client();
        var objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
        assertThat(objects, hasSize(1));

        var object = s3Client.getObject(bucketName, objects.get(0).getKey());
        assertThat(object.getKey(), equalTo(FILE_PATH));

        var actCsv = IOUtils.toString(object.getObjectContent());
        assertThat(actCsv, equalTo(expContent));
    }

    private Map<String, String> createConfigMap(String outputDir, ConsumerGuarantee guarantee) {
        return Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_S3.name(),
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                CONSUMER_BASE_GUARANTEE_PROPERTY, guarantee.name(),
                CONSUMER_S3_FORMAT_TYPE_PROPERTY, FileFormat.CSV.name(),
                CONSUMER_S3_FORMAT_CSV_S3_KEY_NAME_PROPERTY, S3ConsumerCsvTest.FILE_PATH,
                CONSUMER_S3_OUTPUT_DIR_PROPERTY, outputDir,
                CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY, bucketName,
                CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion(),
                CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY, s3Mock.getServiceEndpoint(),
                CONSUMER_S3_AWS_AUTH_TYPE_PROPERTY, S3AuthType.AUTO.name());
    }
}