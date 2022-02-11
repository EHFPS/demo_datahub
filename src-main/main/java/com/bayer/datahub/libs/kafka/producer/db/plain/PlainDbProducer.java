package com.bayer.datahub.libs.kafka.producer.db.plain;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.interfaces.DbContext;
import com.bayer.datahub.libs.interfaces.Producer;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.common.statistics.StatisticsTimer;
import com.bayer.datahub.libs.services.fileio.SimpleRecordManager;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_PLAIN_RANGE_COLUMN_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_PLAIN_RANGE_MAX_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_PLAIN_RANGE_MIN_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.OUTGOING_BYTE_RATE_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.RECORD_RETRY_TOTAL_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.RECORD_SEND_TOTAL_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.getLongProducerMetricValue;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.KAFKA_GROUP;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

class PlainDbProducer implements Producer {
    private static final Logger log = LoggerFactory.getLogger(PlainDbProducer.class);
    private final String query;
    private final MetadataService metadataService;
    private final DbContext context;
    private final String metadataRecordKey;
    private final StatisticsTimer statisticsTimer;
    private final String topic;
    private final ProducerFactory producerFactory;
    private final SimpleRecordManager simpleRecordManager = new SimpleRecordManager();
    private final RecordService recordService;
    private final boolean rangeEnabled;
    private final String rangeColumn;
    private final String rangeMin;
    private final String rangeMax;
    private final StatisticsAggregator statisticsAggregator;
    private KafkaProducer<String, GenericRecord> producer;

    @Inject
    public PlainDbProducer(DbContext context,
                           @Named(MAIN_PRODUCER_FACTORY) ProducerFactory producerFactory,
                           RecordService recordService,
                           MetadataService metadataService, StatisticsAggregator statisticsAggregator,
                           StatisticsTimer statisticsTimer, Configs configs) {
        this.context = context;
        this.metadataService = metadataService;
        this.producerFactory = producerFactory;
        this.recordService = recordService;
        this.statisticsAggregator = statisticsAggregator;
        this.statisticsTimer = statisticsTimer;
        topic = configs.kafkaCommonTopic;
        metadataRecordKey = configs.producerDbPlainMetadataRecordKey;
        rangeEnabled = configs.producerDbPlainRangeEnable;
        rangeColumn = configs.producerDbPlainRangeColumn;
        rangeMin = configs.producerDbPlainRangeMin;
        rangeMax = configs.producerDbPlainRangeMax;
        query = configs.producerDbPlainQuery;
        if (isNotBlank(query)) {
            log.info("Running with custom query: " + query);
        }
    }

    @Override
    public void init() {
        producer = producerFactory.newInstance();
        context.connect();
    }

    @Override
    public void run() {
        try {
            var resultSet = createResultSet();
            while (resultSet.next()) {
                var simpleRecord = simpleRecordManager.buildSimpleRecord(resultSet);
                var genericRecord = recordService.simpleToGenericRecord(simpleRecord);
                sendRecord(genericRecord);
                statisticsTimer.printStatisticsIfTime(data -> statisticsAggregator
                        .title("Intermediate statistics #" + data.getCounter())
                        .addProducerMetrics(producer.metrics(), List.of(RECORD_SEND_TOTAL_METRIC_NAME,
                                RECORD_RETRY_TOTAL_METRIC_NAME, OUTGOING_BYTE_RATE_METRIC_NAME))
                        .getStatistics());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ResultSet createResultSet() {
        log.debug("Creating ResultSet...");
        ResultSet resultSet;
        if (rangeEnabled) {
            if (rangeColumn.isEmpty()) {
                throw new InvalidConfigurationException(PRODUCER_DB_PLAIN_RANGE_COLUMN_PROPERTY, rangeColumn);
            }
            if ((rangeMin.isEmpty() && rangeMax.isEmpty())) {
                throw new InvalidConfigurationException(PRODUCER_DB_PLAIN_RANGE_MIN_PROPERTY + ", "
                        + PRODUCER_DB_PLAIN_RANGE_MAX_PROPERTY, rangeMin + ", " + rangeMax);
            }
            var rangeMinQuoted = format("'%s'", rangeMin);
            var rangeMaxQuoted = format("'%s'", rangeMax);
            if (rangeMax.isEmpty()) {
                resultSet = context.getRecordsWithValueGreater(query, rangeColumn, rangeMinQuoted);
            } else if (rangeMin.isEmpty()) {
                resultSet = context.getRecordsWithValueLess(query, rangeColumn, rangeMaxQuoted);
            } else {
                resultSet = context.getRecordsInRange(query, rangeColumn, rangeMinQuoted, rangeMaxQuoted);
            }
        } else {
            if (query.isEmpty()) {
                if (!metadataService.isEnabled()) {
                    resultSet = context.getRecords();
                } else {
                    var query = metadataService.getQueryWithMetadataTopic();
                    resultSet = context.getRecords(query);
                }
            } else {
                resultSet = context.getRecords(query);
            }
        }
        log.debug("ResultSet is created");
        return resultSet;
    }

    private void sendRecord(GenericRecord record) {
        var producerRecord = new ProducerRecord<>(topic, metadataRecordKey, record);
        producer.send(producerRecord, ((metadata, e) -> {
            if (e != null) {
                log.warn("Exception thrown while sending record: " + e.getMessage());
                statisticsAggregator.failed();
                statisticsAggregator.addThrowable(e);
            }
        }));
    }

    @Override
    public void stop() {
        log.debug("Stopping producer...");
        producer.flush();
        var metrics = producer.metrics();
        var producedRecords = getLongProducerMetricValue(metrics, RECORD_SEND_TOTAL_METRIC_NAME);
        statisticsAggregator
                .finalType()
                .title("Final statistics for " + topic)
                .addKeyValue(KAFKA_GROUP, "Produced records", producedRecords)
                .addProducerMetrics(metrics);
        producer.close();
        log.debug("Producer is stopped");
    }
}
