package com.bayer.datahub;

import com.bayer.datahub.libs.services.json.JsonService;
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
class TestJsonProducer {

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var propertiesFile = "c:/alex/files/kafka_client_cert/kafka_client_cert-iablokov-non-prod-20200512112033/TestJsonProducer.properties";

        var producerProperties = new Properties();
        producerProperties.load(new FileInputStream(propertiesFile));
        var topic = producerProperties.getProperty(KAFKA_COMMON_TOPIC_PROPERTY);
        producerProperties.remove(KAFKA_COMMON_TOPIC_PROPERTY);//Prevent warning

        var now = LocalDateTime.now();
        var nowStr = now.toString().replace(":", "_").replace(".", "_");

        var object = new Person("John", 30, nowStr);
        var jsonService = new JsonService();
        var json = jsonService.writeValue(object);
        var value = json.getBytes();

        var key = "key-" + nowStr;
        var producerRecord = new ProducerRecord<>(topic, 0, key, value);

        try (var producer = new KafkaProducer<String, byte[]>(producerProperties)) {
            producer.send(producerRecord).get();
            System.out.printf("A record is produced: topic='%s', key='%s', value='%s'\n", topic, key, json);
        }
    }

    private static class Person {
        private final String name;
        private final Integer age;
        private final String updated;

        public Person(String name, Integer age, String updated) {
            this.name = name;
            this.age = age;
            this.updated = updated;
        }

        public String getName() {
            return name;
        }

        public Integer getAge() {
            return age;
        }

        public String getUpdated() {
            return updated;
        }
    }
}
