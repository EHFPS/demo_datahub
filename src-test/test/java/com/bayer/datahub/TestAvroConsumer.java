package com.bayer.datahub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Random;

import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_TOPIC_PROPERTY;
import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

/**
 * Consume Avro records from a topic from the beginning (for manual testing purposes).
 */
class TestAvroConsumer {
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException {
        var propertiesFile = "c:/alex/files/kafka_client_cert/kafka_client_cert-iablokov-non-prod-20200512112033/TestAvroConsumer.properties";
        var properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AVRO_DESERIALIZER);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, TestAvroConsumer.class.getSimpleName() + "_" + random.nextInt());

        try (var consumer = new KafkaConsumer<String, Object>(properties)) {
            var topic = properties.getProperty(KAFKA_COMMON_TOPIC_PROPERTY);
            consumer.subscribe(singletonList(topic));

            var tp = new TopicPartition(topic, 0);

            var committed = consumer.committed(singleton(tp));
            var metadata = committed.get(tp);
            long currentOffset;
            if (metadata != null) {
                currentOffset = metadata.offset();
            } else {
                var beginningOffsets = consumer.beginningOffsets(singleton(tp));
                currentOffset = beginningOffsets.get(tp);
            }
            System.out.println("Initial current offset: " + currentOffset);

            boolean finished;
            do {
                var endOffsets = consumer.endOffsets(singleton(tp));
                var endOffset = endOffsets.getOrDefault(tp, 0L);
                System.out.println("End offset: " + endOffset);

                finished = currentOffset == endOffset - 1;

                var records = consumer.poll(Duration.ofSeconds(5));
                System.out.println("Polled records: " + records.count());
                for (var record : records) {
                    currentOffset = record.offset();
                    System.out.println("Current offset: " + currentOffset);
                    var key = record.key();
                    var value = record.value();
                    System.out.println("key: " + key + ", value: " + value);
                }
            } while (!finished);
        }
    }
}
