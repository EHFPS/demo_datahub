package com.bayer.datahub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_TOPIC_PROPERTY;

/**
 * Produces one binary record to a topic (for manual testing purposes).
 */
class TestBinaryProducer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var propertiesFile = "c:/alex/files/kafka_client_cert/kafka_client_cert-iablokov-non-prod-20200512112033/TestBinaryProducer.properties";

        var producerProperties = new Properties();
        producerProperties.load(new FileInputStream(propertiesFile));
        var topic = producerProperties.getProperty(KAFKA_COMMON_TOPIC_PROPERTY);
        producerProperties.remove(KAFKA_COMMON_TOPIC_PROPERTY);//Prevent warning

        var now = LocalDateTime.now();
        var nowStr = now.toString().replace(":", "_").replace(".", "_");
        var key = "key-" + nowStr;
        var valueStr = "value-" + nowStr;
        var value = valueStr.getBytes();

        var fileName = "test-binary-producer/file-" + nowStr;
        var headerName = "filename";
        var fileNameHeader = new RecordHeader(headerName, fileName.getBytes());
        var headers = new RecordHeaders(Collections.singletonList(fileNameHeader));

        var producerRecord = new ProducerRecord<>(topic, 0, key, value, headers);

        try (var producer = new KafkaProducer<String, byte[]>(producerProperties)) {
            producer.send(producerRecord).get();
            System.out.printf("A record is produced: topic='%s', key='%s', header='%s=%s', value='%s'\n",
                    topic, key, headerName, fileName, valueStr);
        }
    }
}
