package com.bayer.datahub.libs.kafka.chunk;

import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SyncAsyncProducer<K, V> implements AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(SyncAsyncProducer.class);
    private final Producer<K, V> producer;
    private final StatisticsAggregator statisticsAggregator;
    private final List<Future<RecordMetadata>> futures = new ArrayList<>();

    public SyncAsyncProducer(Producer<K, V> producer, StatisticsAggregator statisticsAggregator) {
        this.producer = producer;
        this.statisticsAggregator = statisticsAggregator;
    }

    public synchronized void sendAsync(ProducerRecord<K, V> record) {
        futures.add(producer.send(record));
        statisticsAggregator.incrementCounter("SyncAsyncProducer-received-for-sending-async");
    }

    public synchronized void sendSync(List<ProducerRecord<K, V>> records) {
        try {
            statisticsAggregator.incrementCounter("SyncAsyncProducer-received-for-sending-sync", records.size());
            waitAsyncSendingFinish();
            for (ProducerRecord<K, V> record : records) {
                producer.send(record).get();
                statisticsAggregator.incrementCounter("SyncAsyncProducer-sent-sync");
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private synchronized void waitAsyncSendingFinish() {
        try {
            for (Future<RecordMetadata> future : futures) {
                future.get();
                statisticsAggregator.incrementCounter("SyncAsyncProducer-sent-async");
            }
            futures.clear();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void flush() {
        producer.flush();
    }

    public synchronized Map<MetricName, ? extends Metric> metrics() {
        return producer.metrics();
    }

    @Override
    public synchronized void close() {
        waitAsyncSendingFinish();
        producer.close();
        log.debug("SyncAsyncProducer closed");
    }
}
