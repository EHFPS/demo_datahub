package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_TOPIC_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.FilenameGenerator.FILENAME_KAFKA_HEADER;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocLatestKeyConsumer.LATEST_KEY_KAFKA_HEADER;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.RECORD_SEND_TOTAL_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.getLongProducerMetricValue;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.KAFKA_GROUP;

class MetadataProcessor {
    private static final Logger log = LoggerFactory.getLogger(MetadataProcessor.class);
    private final FilenameGenerator filenameGenerator;
    private final ShareDocJsonService shareDocJsonService;
    private final StatisticsAggregator statisticsAggregator;
    private final String topic;
    private final ProducerFactory producerFactory;

    @Inject
    public MetadataProcessor(FilenameGenerator filenameGenerator,
                             @Named(MAIN_PRODUCER_FACTORY) ProducerFactory producerFactory,
                             ShareDocJsonService shareDocJsonService, StatisticsAggregator statisticsAggregator,
                             Configs configs) {
        this.filenameGenerator = filenameGenerator;
        this.shareDocJsonService = shareDocJsonService;
        this.statisticsAggregator = statisticsAggregator;
        topic = configs.kafkaCommonTopic;
        if (StringUtils.isBlank(topic)) {
            throw new InvalidConfigurationException(KAFKA_COMMON_TOPIC_PROPERTY, configs.kafkaCommonTopic);
        }
        this.producerFactory = producerFactory;
    }

    public void loadAndProduceMetadata(Metadata metadata, String lastKey) throws ExecutionException, InterruptedException {
        var partition = 0;
        String key = null;
        try (var producer = producerFactory.newInstance()) {
            var sendFutureList = metadata.getRows().stream()
                    .map(row -> {
                        var value = shareDocJsonService.serializeMetadataRowProperties(row);
                        Header latestKeyHeader = new RecordHeader(LATEST_KEY_KAFKA_HEADER, lastKey.getBytes());
                        var filename = filenameGenerator.getMetadataFilePath(row);
                        Header filenameHeader = new RecordHeader(FILENAME_KAFKA_HEADER, filename.getBytes());
                        Iterable<Header> headers = Arrays.asList(latestKeyHeader, filenameHeader);
                        var producerRecord = new ProducerRecord<Object, Object>(topic, partition, key, value, headers);
                        var recordStr = String.format("ProducerRecord<String, String>" +
                                        "(topic='%s', partition=%s, key='%s', value_length=%s, " +
                                        "latest_key_header_name='%s', latest_key_header_value='%s', " +
                                        "filename_header_name='%s', filename_header_value='%s')",
                                topic, partition, key, value.length(), LATEST_KEY_KAFKA_HEADER, lastKey,
                                FILENAME_KAFKA_HEADER, filename);
                        log.debug("Sending {}...", recordStr);
                        return producer.send(producerRecord);
                    }).collect(Collectors.toList());
            for (var future : sendFutureList) {
                future.get();
                statisticsAggregator.incrementCounter("MetadataProcessor-sent-records");
            }
            producer.flush();
            var metrics = producer.metrics();
            var producedRecords = getLongProducerMetricValue(metrics, RECORD_SEND_TOTAL_METRIC_NAME);
            statisticsAggregator
                    .finalType()
                    .title("Final statistics for " + topic)
                    .addKeyValue(KAFKA_GROUP, "Produced records", producedRecords)
                    .addProducerMetrics(metrics);
        }
    }

}
