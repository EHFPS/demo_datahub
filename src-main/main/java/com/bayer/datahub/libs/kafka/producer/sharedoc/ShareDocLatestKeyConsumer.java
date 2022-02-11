package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.bayer.datahub.libs.kafka.producer.BaseLatestKeyConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

class ShareDocLatestKeyConsumer extends BaseLatestKeyConsumer<String, String> {
    private static final Logger log = LoggerFactory.getLogger(ShareDocLatestKeyConsumer.class);
    public static final String LATEST_KEY_KAFKA_HEADER = "latestKey";

    @Inject
    ShareDocLatestKeyConsumer(Configs configs, ConsumerFactory consumerFactory) {
        super(configs, consumerFactory);
    }

    @Override
    protected String extractLatestKey(ConsumerRecord<String, String> latestRecord) {
        String latestKey = null;
        var latestKeyHeader = latestRecord.headers().lastHeader(LATEST_KEY_KAFKA_HEADER);
        if (latestKeyHeader != null) {
            latestKey = new String(latestKeyHeader.value());
        } else {
            log.debug("'{}' Kafka header in the last record is null", LATEST_KEY_KAFKA_HEADER);
        }
        return latestKey;
    }
}
