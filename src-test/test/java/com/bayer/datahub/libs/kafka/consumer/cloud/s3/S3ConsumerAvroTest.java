package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.AT_LEAST_ONCE;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.EXACTLY_ONCE;
import static com.bayer.datahub.libs.services.fileio.FileFormat.AVRO;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Map.entry;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

class S3ConsumerAvroTest extends AbstractKafkaBaseTest {
    private static final S3Mock s3Mock = new S3Mock();
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());

    @Test
    void exactlyOnce() throws InterruptedException, ExecutionException, IOException {
        runTest(EXACTLY_ONCE);
    }

    @Test
    void atLeastOnce() throws IOException, ExecutionException, InterruptedException {
        runTest(AT_LEAST_ONCE);
    }

    private void runTest(ConsumerGuarantee guarantee) throws IOException, ExecutionException, InterruptedException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var producer = KAFKA_CLIENT_HELPER.createAvroProducer();

        var fieldNameA = "columnA";
        var fieldNameB = "columnB";
        var schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name(fieldNameA).type().stringType().noDefault()
                .name(fieldNameB).type().stringType().noDefault()
                .endRecord();

        var fieldValueA1 = "Desk1";
        var fieldValueB1 = "2019-06-10 00:00:00";
        GenericRecord expRecord1 = new GenericData.Record(schema);
        expRecord1.put(fieldNameA, fieldValueA1);
        expRecord1.put(fieldNameB, fieldValueB1);
        var record1 = new ProducerRecord<String, Object>(topic, expRecord1);

        var fieldValueA2 = "Desk2";
        var fieldValueB2 = "2020-06-10 00:00:00";
        GenericRecord expRecord2 = new GenericData.Record(schema);
        expRecord2.put(fieldNameA, fieldValueA2);
        expRecord2.put(fieldNameB, fieldValueB2);
        var record2 = new ProducerRecord<String, Object>(topic, expRecord2);

        var fieldValueA3 = "Desk3";
        var fieldValueB3 = "2021-06-10 00:00:00";
        GenericRecord expRecord3 = new GenericData.Record(schema);
        expRecord3.put(fieldNameA, fieldValueA3);
        expRecord3.put(fieldNameB, fieldValueB3);
        var record3 = new ProducerRecord<String, Object>(topic, expRecord3);

        producer.send(record1).get();
        producer.send(record2).get();
        producer.send(record3).get();

        var overrideConsumerConfig = new Properties();
        overrideConsumerConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2);

        var bucketName = s3Mock.createRandomBucket();
        s3Mock.getS3Client().createBucket(bucketName);

        var outputDir = createTempDirectory(S3ConsumerAvroTest.class.getSimpleName());
        var filePath = "subdir/avroFormat.avro";
        var config = createConfigs(AVRO.name(), filePath, outputDir.toString(), topic, bucketName, guarantee);
        var s3ClientFactory = s3Mock.getS3ClientFactoryMock();
        var configs = new Configs(config);//TODO remove
        var s3FileUploader = spy(new S3FileUploader(s3ClientFactory, configs));
        var s3Consumer = FactoryBuilder.newBuilder(config)
                .withStringProducerFactory()
                .withAvroConsumerFactory(overrideConsumerConfig)
                .override(CloudClientFactory.class, s3Mock.getS3ClientFactoryMock())
                .override(S3FileUploader.class, s3FileUploader)
                .withSchemaRegistryMock(schema)
                .build().getKafkaClient();
        s3Consumer.init();
        s3Consumer.run();
        s3Consumer.stop();

        verify(s3FileUploader, times(1)).uploadDir(outputDir);

        var s3Client = s3Mock.getS3Client();
        var objects = s3Client.listObjectsV2(bucketName).getObjectSummaries();
        assertThat(objects, hasSize(1));

        var object = s3Client.getObject(bucketName, objects.get(0).getKey());
        assertThat(object.getKey(), equalTo(filePath));

        InputStream is = object.getObjectContent();
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>(schema);
        var dataFileReader = new DataFileStream<>(is, datumReader);
        GenericRecord actRecord = new GenericData.Record(schema);

        Assertions.assertTrue(dataFileReader.hasNext());
        dataFileReader.next(actRecord);
        assertThat(actRecord.get(fieldNameA).toString(), equalTo(fieldValueA1));
        assertThat(actRecord.get(fieldNameB).toString(), equalTo(fieldValueB1));

        Assertions.assertTrue(dataFileReader.hasNext());
        dataFileReader.next(actRecord);
        assertThat(actRecord.get(fieldNameA).toString(), equalTo(fieldValueA2));
        assertThat(actRecord.get(fieldNameB).toString(), equalTo(fieldValueB2));

        Assertions.assertTrue(dataFileReader.hasNext());
        dataFileReader.next(actRecord);
        assertThat(actRecord.get(fieldNameA).toString(), equalTo(fieldValueA3));
        assertThat(actRecord.get(fieldNameB).toString(), equalTo(fieldValueB3));
    }

    private Map<String, String> createConfigs(String fileFormat, String filePath, String outputDir, String topic, String bucketName,
                                              ConsumerGuarantee guarantee) {
        return Map.ofEntries(
                entry(CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_S3.name()),
                entry(KAFKA_COMMON_TOPIC_PROPERTY, topic),
                entry(CONSUMER_BASE_GUARANTEE_PROPERTY, guarantee.name()),
                entry(CONSUMER_S3_FORMAT_TYPE_PROPERTY, fileFormat),
                entry(CONSUMER_S3_FORMAT_AVRO_S3_KEY_NAME_PROPERTY, filePath),
                entry(CONSUMER_S3_OUTPUT_DIR_PROPERTY, outputDir),
                entry(CONSUMER_S3_FORMAT_BINARY_HEADER_NAME_PROPERTY, ""),
                entry(CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY, bucketName),
                entry(CONSUMER_S3_AWS_REGION_PROPERTY, s3Mock.getRegion()),
                entry(CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY, s3Mock.getServiceEndpoint()),
                entry(CONSUMER_S3_AWS_AUTH_TYPE_PROPERTY, S3AuthType.AUTO.name()));
    }
}