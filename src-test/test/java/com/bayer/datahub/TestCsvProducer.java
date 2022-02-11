package com.bayer.datahub;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_TOPIC_PROPERTY;

/**
 * Produces one JSON record to a topic (for manual testing purposes).
 */
class TestCsvProducer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var propertiesFile = "c:/alex/files/kafka_client_cert/kafka_client_cert-iablokov-non-prod-20200512112033/TestCsvProducer.properties";

        var producerProperties = new Properties();
        producerProperties.load(new FileInputStream(propertiesFile));
        var topic = producerProperties.getProperty(KAFKA_COMMON_TOPIC_PROPERTY);
        producerProperties.remove(KAFKA_COMMON_TOPIC_PROPERTY);//Prevent warning

        var now = LocalDateTime.now();
        var nowStr = now.toString().replace(":", "_").replace(".", "_");
        var csv = String.format("city,population,created\nNewYork,1000000,%s", nowStr);
        var value = csv.getBytes();

        var key = "key-" + nowStr;
        var producerRecord = new ProducerRecord<>(topic, 0, key, value);

        try (var producer = new KafkaProducer<String, byte[]>(producerProperties)) {
            producer.send(producerRecord).get();
            System.out.printf("A record is produced: topic='%s', key='%s', value='%s'\n", topic, key, csv);
        }
    }
}
