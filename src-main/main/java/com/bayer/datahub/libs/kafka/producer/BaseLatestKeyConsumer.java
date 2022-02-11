package com.bayer.datahub.libs.kafka.producer;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.Collections;

/**
 * Retrieves the last record from a topic and extracts the latest key from it.
 */
public abstract class BaseLatestKeyConsumer<K, V> {
    private static final Logger log = LoggerFactory.getLogger(BaseLatestKeyConsumer.class);
    private final ConsumerFactory consumerFactory;
    private final String topic;

    @Inject
    protected BaseLatestKeyConsumer(Configs configs, ConsumerFactory consumerFactory) {
        log.debug("Creating ShareDocLatestKeyConsumer...");
        this.consumerFactory = consumerFactory;
        this.topic = configs.kafkaCommonTopic;
    }

    public String getLatestKey() {
        log.debug("Finding latest key in '{}' topic...", topic);
        try (var consumer = (KafkaConsumer<K, V>) consumerFactory.newInstance()) {
            var tp = new TopicPartition(topic, 0);
            var tpList = Collections.singletonList(tp);
            consumer.assign(tpList);
            var endOffset = consumer.endOffsets(tpList).get(tp);
            var beginningOffset = consumer.beginningOffsets(tpList).get(tp);

            String latestKey = null;
            if (endOffset > beginningOffset) {
                consumer.seek(tp, endOffset - 1);
                ConsumerRecords<K, V> records;
                do {
                    records = consumer.poll(Duration.ofMillis(1000));
                } while (records.isEmpty());
                var recordList = records.records(tp);
                var latestRecord = recordList.get(recordList.size() - 1);
                latestKey = extractLatestKey(latestRecord);
            }

            log.info("Latest key in topic '{}': '{}'", topic, latestKey);
            return latestKey;
        }
    }

    protected abstract String extractLatestKey(ConsumerRecord<K, V> latestRecord);
}
