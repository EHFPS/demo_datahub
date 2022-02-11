package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.interfaces.DbContext;
import com.bayer.datahub.libs.interfaces.Producer;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.fileio.SimpleRecordManager;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_COLUMN_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_TYPE_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.db.delta.DbLatestKeyConsumer.LATEST_KEY_KAFKA_HEADER;
import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.RECORD_SEND_TOTAL_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.getLongProducerMetricValue;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.KAFKA_GROUP;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isBlank;

class DeltaDbProducer implements Producer {
    private static final Logger log = LoggerFactory.getLogger(DeltaDbProducer.class);
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final DbContext dbContext;
    private final Integer pollInterval;
    private final String query;
    private final String topic;
    private final ProducerFactory producerFactory;
    private final SimpleRecordManager simpleRecordManager = new SimpleRecordManager();
    private final RecordService recordService;
    private final DeltaType type;
    private final String deltaColumn;
    private final ConditionService conditionService;
    private final StatisticsAggregator statisticsAggregator;
    private KafkaProducer<String, GenericRecord> producer;
    private LocalDateTime latestKafkaDate;

    @Inject
    DeltaDbProducer(DbContext dbContext,
                    @Named(MAIN_PRODUCER_FACTORY) ProducerFactory producerFactory,
                    RecordService recordService,
                    ConditionService conditionService, StatisticsAggregator statisticsAggregator, Configs configs) {
        this.dbContext = dbContext;
        this.producerFactory = producerFactory;
        this.recordService = recordService;
        this.conditionService = conditionService;
        this.statisticsAggregator = statisticsAggregator;
        topic = configs.kafkaCommonTopic;
        query = configs.producerDbDeltaQuery;
        pollInterval = configs.producerDbDeltaPollInterval;
        type = DeltaType.parse(configs.producerDbDeltaType);
        deltaColumn = parseDeltaColumn(configs.producerDbDeltaColumn);
    }

    @Override
    public void init() {
        producer = producerFactory.newInstance();
        dbContext.connect();
        conditionService.init();
    }

    @Override
    public void run() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping application...");
            stop();
            log.info("Producer closed... Exiting application");
        }));

        if (pollInterval == null) {
            runClient();
        } else {
            while (true) {
                runClient();
                try {
                    TimeUnit.SECONDS.sleep(pollInterval);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        }
    }

    private void runClient() {
        try {
            conditionService.nextIteration();
            while (!conditionService.isDone()) {
                var rs = getRecordResultSet(conditionService.isIterationMinIncludes());
                while (rs.next()) {
                    var simpleRecord = simpleRecordManager.buildSimpleRecord(rs);
                    var genericRecord = recordService.simpleToGenericRecord(simpleRecord);
                    sendRecord(genericRecord);
                }
                conditionService.nextIteration();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String parseDeltaColumn(String producerDbDeltaColumn) {
        if (isBlank(producerDbDeltaColumn)) {
            throw new InvalidConfigurationException(PRODUCER_DB_DELTA_COLUMN_PROPERTY, producerDbDeltaColumn);
        }
        return producerDbDeltaColumn;
    }

    private ResultSet getRecordResultSet(boolean minIncluded) {
        ResultSet rs;
        if (isBlank(query)) {
            var minStr = formatLocalDateTime(conditionService.getIterationMin());
            var maxStr = formatLocalDateTime(conditionService.getIterationMax());
            rs = dbContext.getDeltaRecords(deltaColumn, minStr, maxStr, minIncluded);
        } else {
            log.info("Running with custom query: " + query);
            rs = dbContext.getRecords(query);
        }
        return rs;
    }

    private String formatLocalDateTime(LocalDateTime localDateTime) {
        if (localDateTime == null) {
            return null;
        }
        switch (type) {
            case TIMESTAMP:
                return localDateTime.toString();
            case DATE:
                return dateFormatter.format(localDateTime);
            default:
                throw new InvalidConfigurationException(PRODUCER_DB_DELTA_TYPE_PROPERTY, type, DeltaType.values());
        }
    }

    private void sendRecord(GenericRecord record) {
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        var deltaColumnValue = getDeltaColumnField(record);
        var deltaColumnDate = DateParser.parse(deltaColumnValue.toString());
        if (latestKafkaDate == null || latestKafkaDate.isBefore(deltaColumnDate)) {
            latestKafkaDate = deltaColumnDate;
        }
        var headerValue = formatter.format(latestKafkaDate).getBytes();
        var latestKeyHeader = new RecordHeader(LATEST_KEY_KAFKA_HEADER, headerValue);
        Iterable<Header> headers = singletonList(latestKeyHeader);
        var recordKey = formatter.format(deltaColumnDate);
        var producerRecord = new ProducerRecord<>(topic, 0, recordKey, record, headers);
        producer.send(producerRecord);
    }

    private Object getDeltaColumnField(GenericRecord record) {
        var caseInsensitiveFieldNames = record.getSchema().getFields().stream()
                .map(Schema.Field::name)
                .collect(toMap(String::toLowerCase, fieldName -> fieldName));
        var deltaColumnFieldName = caseInsensitiveFieldNames.get(deltaColumn.toLowerCase());
        var deltaColumnValue = record.get(deltaColumnFieldName);
        if (deltaColumnValue == null) {
            var fields = record.getSchema().getFields().stream().map(Schema.Field::name).toArray();
            throw new InvalidConfigurationException(PRODUCER_DB_DELTA_COLUMN_PROPERTY, deltaColumn, fields);
        }
        return deltaColumnValue;
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
        dbContext.close();
        conditionService.stop();
        log.debug("Producer is stopped");
    }
}
