package com.bayer.datahub.libs.kafka.consumer.es;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.interfaces.Consumer;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_ES_FORMAT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.services.fileio.FileFormat.AVRO;
import static com.bayer.datahub.libs.services.fileio.FileFormat.CSV;

/**
 * EsConsumer reads messages from Avro topic and indexes its to an ElasticSearch index.
 */
class EsConsumer implements Consumer {
    private static final Logger log = LoggerFactory.getLogger(EsConsumer.class);
    private final RecordConsumer recordConsumer;
    private final EsService esService;
    private final RecordService recordService;
    private final FileFormat format;

    @Inject
    public EsConsumer(RecordConsumer recordConsumer, EsService esService, RecordService recordService, Configs configs) {
        this.recordConsumer = recordConsumer;
        this.esService = esService;
        this.recordService = recordService;
        this.format = configs.consumerEsFormatType;
    }

    @Override
    public void init() {
        recordConsumer.init();
        esService.init();
    }

    @Override
    public void run() {
        recordConsumer.run((consumerRecords -> {
            if (consumerRecords.isEmpty()) {
                return;
            }
            log.info("Indexing records to ElasticSearch...");
            switch (format) {
                case AVRO:
                    var genericRecords = recordService.consumerRecordsToGenericRecords(
                            (ConsumerRecords<?, GenericRecord>) consumerRecords);
                    for (var record : genericRecords) {
                        esService.indexGenericRecord(record);
                    }
                    break;
                case CSV:
                    var csv = recordService.consumerRecordsToCsv((ConsumerRecords<?, String>) consumerRecords);
                    esService.indexCsv(csv);
                    break;
                case JSON:
                    var jsonList = recordService.consumerRecordsToStringArray((ConsumerRecords<?, String>) consumerRecords);
                    for (String json : jsonList) {
                        esService.indexJson(json);
                    }
                    break;
                default:
                    throw new InvalidConfigurationException(CONSUMER_ES_FORMAT_TYPE_PROPERTY, format,
                            new Object[]{AVRO, CSV});
            }
            recordConsumer.commit();
            log.info("Records are indexed: {} records", consumerRecords.count());
        }));
    }

    @Override
    public void stop() {
        recordConsumer.stop();
        esService.stop();
    }
}
