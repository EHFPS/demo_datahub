package com.bayer.datahub.libs.kafka.consumer.file;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.kafkaclientbuilder.ConsumerPropertiesGenerator;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.google.common.io.Files;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class FileConsumerTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());

    private static final Properties OVERRIDE_CONSUMER_CONFIG = ConsumerPropertiesGenerator.genPropertiesWithGroupId();

    @Test
    void csvFormat() throws IOException, ExecutionException, InterruptedException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var filePath = "data/content.csv";
        var outputDir = createTempDirectory("csvFormat");

        //1st iteration (new consumer group)
        var fieldValueA1 = "Desk";
        var fieldValueB1 = "2019-06-10 00:00:00";
        consumeAndAssert(topic, outputDir, filePath, fieldValueA1, fieldValueB1);

        //2nd iteration (existing consumer group)
        var fieldValueA2 = "Desk2";
        var fieldValueB2 = "2020-06-10 00:00:00";
        consumeAndAssert(topic, outputDir, filePath, fieldValueA2, fieldValueB2);
    }

    @Test
    void binaryFormat() throws IOException, ExecutionException, InterruptedException {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var headerName = "filename";

        var fileName1 = "out-1.bin";
        var expContentStr1 = "the content 1";

        var fileName2 = "out-2.bin";
        var expContentStr2 = "the content 2";

        var producer = KAFKA_CLIENT_HELPER.createBinaryProducer();
        produceBinaryRecord(producer, topic, headerName, fileName1, expContentStr1);
        produceBinaryRecord(producer, topic, headerName, fileName2, expContentStr2);

        var outputDir = createTempDirectory(FileConsumerTest.class.getSimpleName());
        var consumerFactory = KAFKA_CLIENT_HELPER.createBinaryConsumerFactory(OVERRIDE_CONSUMER_CONFIG);
        var fileConsumer = createFileConsumer(topic, consumerFactory, "binary",
                "", outputDir.toString(), headerName, null);
        runFileConsumer(fileConsumer);

        assertBinaryFile(outputDir, fileName1, expContentStr1);
        assertBinaryFile(outputDir, fileName2, expContentStr2);
    }

    private void assertBinaryFile(Path outputDir, String fileName, String expContentStr) throws IOException {
        var expFile1 = outputDir.resolve(fileName);
        var actContent1 = Files.asByteSource(expFile1.toFile()).read();
        var actContentStr = new String(actContent1);
        assertThat(actContentStr, equalTo(expContentStr));
    }

    private void produceBinaryRecord(KafkaProducer<String, byte[]> producer, String topic, String headerName,
                                     String fileName, String valueStr) throws InterruptedException, ExecutionException {
        var recordKey = "the-key";
        var expContent = valueStr.getBytes();
        Header fileNameHeader = new RecordHeader(headerName, fileName.getBytes());
        Headers headers = new RecordHeaders(Collections.singletonList(fileNameHeader));
        var record = new ProducerRecord<>(topic, 0, recordKey, expContent, headers);
        producer.send(record).get();
    }

    private void consumeAndAssert(String topic, Path outputDir, String filePath, String fieldValueA, String fieldValueB)
            throws InterruptedException, ExecutionException, IOException {
        var fieldNameA = "columnA";
        var fieldNameB = "columnB";
        var schema = createSchema(fieldNameA, fieldNameB);
        GenericRecord expRecord = new GenericData.Record(schema);
        expRecord.put(fieldNameA, fieldValueA);
        expRecord.put(fieldNameB, fieldValueB);

        var producer = KAFKA_CLIENT_HELPER.createAvroProducer();
        var record = new ProducerRecord<String, Object>(topic, expRecord);
        producer.send(record).get();

        var consumerFactory = KAFKA_CLIENT_HELPER.createAvroConsumerFactory(OVERRIDE_CONSUMER_CONFIG);
        var fileConsumer = createFileConsumer(topic, consumerFactory, "csv", filePath,
                outputDir.toString(), "", schema);
        runFileConsumer(fileConsumer);

        var actContent = Files.asCharSource(Paths.get(outputDir.toString(), filePath).toFile(), Charset.defaultCharset()).read();
        assertThat(actContent, equalTo(format("%s,%s\n%s,%s\n", fieldNameA, fieldNameB, fieldValueA, fieldValueB)));
    }

    private static Schema createSchema(String fieldNameA, String fieldNameB) {
        return SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name(fieldNameA).type().stringType().noDefault()
                .name(fieldNameB).type().stringType().noDefault()
                .endRecord();
    }

    private static void runFileConsumer(KafkaClient fileConsumer) {
        fileConsumer.init();
        fileConsumer.run();
        fileConsumer.stop();
    }

    private static KafkaClient createFileConsumer(String topic, ConsumerFactory consumerFactory, String fileFormat,
                                                  String filePath, String outputDir, String headerName, Schema schema) {
        return FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_FILE.name(),
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                CONSUMER_FILE_FILE_FORMAT_PROPERTY, fileFormat,
                CONSUMER_FILE_FILE_PATH_PROPERTY, filePath,
                CONSUMER_FILE_BINARY_OUTPUT_DIR_PROPERTY, outputDir,
                CONSUMER_S3_FORMAT_BINARY_HEADER_NAME_PROPERTY, headerName))
                .override(ConsumerFactory.class, consumerFactory)
                .withSchemaRegistryMock(schema)
                .build().getKafkaClient();
    }
}