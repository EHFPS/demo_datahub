package com.bayer.datahub.libs.kafka.consumer;

import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.common.statistics.StatisticsTimer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;

import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.AT_LEAST_ONCE;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.EXACTLY_ONCE;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;

/**
 * {@link RecordConsumer} consumes new records from a topic and invoke {@link Handler} for its.
 */
public class RecordConsumer {
    private static final Logger log = LoggerFactory.getLogger(RecordConsumer.class);
    private final String topic;
    private final ConsumerFactory consumerFactory;
    private final StatisticsTimer statisticsTimer;
    private final RecordMetadata recordMetadata;
    private final String deserializerProperty;
    private final boolean includeMetadata;
    private final long offset;
    private final int partition;
    private final long pollTimeoutMs;
    private final boolean exitOnFinish;
    private final ConsumerGuarantee consumerGuarantee;
    private final StatisticsAggregator statisticsAggregator;
    private KafkaConsumer<?, ?> consumer;
    private boolean initialized = false;

    @Inject
    public RecordConsumer(ConsumerFactory consumerFactory, StatisticsAggregator statisticsAggregator,
                          StatisticsTimer statisticsTimer, RecordConsumerConfig rcConfig, RecordMetadata metadata) {
        this.consumerFactory = consumerFactory;
        this.statisticsTimer = statisticsTimer;
        topic = rcConfig.topic;
        offset = rcConfig.offset;
        partition = rcConfig.partition;
        pollTimeoutMs = rcConfig.pollTimeoutMs;
        consumerGuarantee = rcConfig.guarantee;
        this.exitOnFinish = rcConfig.exitOnFinish;
        log.debug("ConsumerGuarantee: " + consumerGuarantee);
        this.statisticsAggregator = statisticsAggregator;
        this.includeMetadata = rcConfig.includeMetadata;
        this.deserializerProperty = rcConfig.deserializerProperty;
        this.recordMetadata = metadata;
    }

    private static ConsumerRecords wrapRecordToRecords(ConsumerRecord<?, ?> record) {
        var map = new HashMap<TopicPartition, List<ConsumerRecord<?, ?>>>();
        map.put(new TopicPartition(record.topic(), record.partition()), singletonList(record));
        return new ConsumerRecords(map);
    }

    public void init() {
        if (initialized) {
            throw new IllegalStateException(getClass().getSimpleName() + " is already initialized");
        }
        consumer = consumerFactory.newInstance();
        subscribe();
        initialized = true;
    }

    public void run(Handler atLeastOnceRecordsHandler) {
        Handler exactlyOnceRecordsHandler = (records) -> {
            for (var record : records) {
                atLeastOnceRecordsHandler.process(wrapRecordToRecords(record));
            }
        };
        run(atLeastOnceRecordsHandler, exactlyOnceRecordsHandler);
    }

    public void run(Handler atLeastOnceRecordsHandler,
                    Handler exactlyOnceRecordsHandler) {
        log.trace("Entering RecordConsumer#run()");
        if (!initialized) {
            throw new IllegalStateException(getClass().getSimpleName() + " is not initialized");
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            stop();
            log.info("Consumer closed... Exiting application");
        }));

        try {
            var tp = new TopicPartition(topic, 0);
            var currentOffset = getInitialOffset(tp);
            boolean finished;
            var consumedRecordsKey = "Consumed-records";
            statisticsAggregator.incrementCounter(consumedRecordsKey, 0);
            log.debug("Actual poll.timeout.ms: {}", pollTimeoutMs);
            do {
                long endOffset = getEndOffset(tp);
                finished = currentOffset >= endOffset;
                log.debug("Finished: {}", finished);
                if (!finished) {
                    var records = consumer.poll(Duration.ofMillis(pollTimeoutMs));
                    log.debug("Received records: {}", records.count());
                    if (!records.isEmpty()) {
                        currentOffset = records.records(tp).get(records.count() - 1).offset() + 1;
                        log.debug("Current offset: {}", currentOffset);
                    }
                    if(includeMetadata) {
                        records = enrichRecords(records, deserializerProperty);
                    }
                    if (consumerGuarantee == AT_LEAST_ONCE) {
                        atLeastOnceRecordsHandler.process(records);
                    } else if (consumerGuarantee == EXACTLY_ONCE) {
                        exactlyOnceRecordsHandler.process(records);
                    } else {
                        throw new IllegalArgumentException("Unknown ConsumerGuarantee: " + consumerGuarantee);
                    }
                    statisticsAggregator.incrementCounter(consumedRecordsKey, records.count());
                    statisticsTimer.printStatisticsIfTime(data -> statisticsAggregator
                            .title("Intermediate statistics #" + data.getCounter())
                            .getStatistics());
                }
            } while (!finished || !exitOnFinish);
        } catch (WakeupException ex) {
            log.info("Closing consumer...");
        } finally {
            statisticsAggregator
                    .finalType()
                    .title("Final statistics for " + topic)
                    .addConsumerMetrics(consumer.metrics());
        }
        log.trace("Exiting RecordConsumer#run()");
    }

    public void stop() {
        if (consumer != null) {
            consumer.wakeup();
            consumer.close();
        }
    }

    public void commit() {
        log.trace("Entering RecordConsumer#commit()");
        log.info("Committing offset...");
        consumer.commitSync();
        log.trace("Exiting RecordConsumer#commit()");
    }

    private void subscribe() {
        if (offset == -1) {
            log.debug("Subscribing to topic '{}'...", topic);
            consumer.subscribe(singleton(topic));
            log.debug("Subscribed to topic '{}'", topic);
        } else if (offset >= 0 && partition >= 0) {
            var tp = new TopicPartition(topic, partition);
            consumer.assign(singleton(tp));
            log.info("Seeking to offset: " + offset);
            consumer.seek(tp, offset);
        }
    }

    private Long getInitialOffset(TopicPartition tp) {
        var tpSet = singleton(tp);

        var beginningOffset = consumer.beginningOffsets(tpSet).get(tp);
        log.debug("Beginning offset: {}", beginningOffset);

        var metadata = consumer.committed(tpSet).get(tp);
        var committedOffset = metadata != null ? metadata.offset() : null;
        log.debug("Committed offset: {}", committedOffset);

        Long initialOffset;
        if (beginningOffset != null && committedOffset != null) {
            initialOffset = Math.max(beginningOffset, committedOffset);
        } else if (committedOffset == null) {
            initialOffset = beginningOffset;
        } else {
            throw new IllegalStateException("Both beginning offset and committed offset are nulls.");
        }

        log.debug("Initial current offset: {}", initialOffset);
        return initialOffset;
    }

    private Long getEndOffset(TopicPartition tp) {
        var endOffsets = consumer.endOffsets(singleton(tp));
        var endOffset = endOffsets.getOrDefault(tp, 0L);
        log.debug("End offset: {}", endOffset);
        return endOffset;
    }

    public interface Handler {
        void process(ConsumerRecords<?, ?> records);
    }

    public String getTopic() {
        return this.topic;
    }

    private ConsumerRecords<?, ?> enrichRecords(ConsumerRecords<?, ?> records, String deserializerProperty) {
        if(deserializerProperty.equalsIgnoreCase("avro"))
            return recordMetadata.enrichGenericRecords((ConsumerRecords<?, GenericRecord>) records);
        else if(deserializerProperty.equalsIgnoreCase("string"))
            return recordMetadata.enrichStringRecords((ConsumerRecords<?, String>) records);
        else
            throw new RuntimeException("Unknown deserializer property " + deserializerProperty + ". Please check your configuration");
    }
}