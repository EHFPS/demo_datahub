package com.bayer.datahub.libs.kafka.producer.csv;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.Producer;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.fileio.FileReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;

class CsvProducer implements Producer {
    private static final Logger log = LoggerFactory.getLogger(CsvProducer.class);
    private final FileReader fileReader;
    private final Path filePath;
    private final RecordService recordService;
    private final String topic;
    private final ProducerFactory producerFactory;
    private final StatisticsAggregator statisticsAggregator;
    private KafkaProducer<String, GenericRecord> producer;

    @Inject
    public CsvProducer(@Named(MAIN_PRODUCER_FACTORY) ProducerFactory producerFactory,
                       RecordService recordService,
                       StatisticsAggregator statisticsAggregator, Configs configs) {
        this.producerFactory = producerFactory;
        this.recordService = recordService;
        this.statisticsAggregator = statisticsAggregator;
        this.topic = configs.kafkaCommonTopic;
        filePath = Paths.get(configs.producerCsvFilePath);
        var fileFormat = configs.producerCsvFileFormat;
        var csvValueDelimiter = configs.producerCsvValueDelimiter;
        var csvRecordDelimiter = configs.producerCsvRecordDelimiter;
        var fileBatchSize = configs.producerCsvFileBatchSize;
        var csvCharset = configs.producerCsvCharset;
        var csvHeader = configs.producerCsvHeader;
        fileReader = FileReader.getInstance(fileFormat, csvValueDelimiter, csvRecordDelimiter, csvCharset, fileBatchSize, csvHeader);
    }

    @Override
    public void init() {
        fileReader.open(filePath);
        producer = producerFactory.newInstance();
    }

    @Override
    public void run() {
        while (!isDone()) {
            var simpleRecords = fileReader.read();
            var records = recordService.simpleToGenericRecords(simpleRecords);
            sendRecords(records);
        }
    }

    private void sendRecords(List<GenericRecord> records) {
        for (var record : records) {
            var producerRecord = new ProducerRecord<String, GenericRecord>(topic, record);
            producer.send(producerRecord, ((metadata, exception) -> {
                if (exception != null) {
                    log.warn("Exception thrown while sending record: " + exception.getMessage());
                }
            }));
        }
    }

    private boolean isDone() {
        return !fileReader.hasNextLine();
    }

    @Override
    public void stop() {
        try {
            fileReader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        producer.flush();
        statisticsAggregator
                .finalType()
                .title("Final statistics for " + topic)
                .addMemoryUsage()
                .addProducerMetrics(producer.metrics());
    }
}
