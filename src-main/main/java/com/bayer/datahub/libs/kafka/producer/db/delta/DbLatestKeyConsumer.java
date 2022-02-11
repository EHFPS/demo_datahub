package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.bayer.datahub.libs.kafka.producer.BaseLatestKeyConsumer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

class DbLatestKeyConsumer extends BaseLatestKeyConsumer<String, GenericRecord> {
    public static final String LATEST_KEY_KAFKA_HEADER = "latestKeyInTopic";
    private static final Logger log = LoggerFactory.getLogger(DbLatestKeyConsumer.class);
    private final String deltaColumn;

    @Inject
    public DbLatestKeyConsumer(Configs configs, ConsumerFactory consumerFactory) {
        super(configs, consumerFactory);
        this.deltaColumn = configs.producerDbDeltaColumn;
    }

    @Override
    protected String extractLatestKey(ConsumerRecord<String, GenericRecord> latestRecord) {
        log.debug("Latest record: " + latestRecord);
        String latestKey;
        var latestKeyHeader = latestRecord.headers().lastHeader(LATEST_KEY_KAFKA_HEADER);
        if (latestKeyHeader != null) {
            latestKey = new String(latestKeyHeader.value());
        } else {
            latestKey = latestRecord.key();
            if (latestKey == null) {
                var value = latestRecord.value();
                var timestamp = value.get(deltaColumn);
                if (timestamp != null) {
                    latestKey = timestamp.toString();
                }
            }
        }
        return latestKey;
    }
}
