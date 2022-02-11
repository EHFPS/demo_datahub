package com.bayer.datahub.libs.kafka.producer.db.plain;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.fileio.SimpleRecord;
import com.bayer.datahub.libs.services.fileio.SimpleRecordManager;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

class MetadataService {
    private static final Logger log = LoggerFactory.getLogger(MetadataService.class);
    private static final String COUNT_FIELD = "count";
    private final SimpleRecordManager simpleRecordManager = new SimpleRecordManager();
    private final String metadataTopic;
    private final String metadataDate;
    private final String metadataRecordKey;
    private final ProducerFactory producerFactory;
    private final ConsumerFactory consumerFactory;
    private final String metadataDateFormat;
    private final String columnOrder;
    private final String tableName;
    private final String metadataColumn;

    @Inject
    public MetadataService(Configs configs,
                           @Named(MAIN_PRODUCER_FACTORY) ProducerFactory producerFactory,
                           ConsumerFactory consumerFactory) {
        this.producerFactory = producerFactory;
        this.consumerFactory = consumerFactory;
        metadataTopic = configs.producerDbPlainMetadataTopic;
        metadataDate = configs.producerDbPlainMetadataDate;
        metadataRecordKey = configs.producerDbPlainMetadataRecordKey;
        metadataDateFormat = configs.producerDbPlainMetadataFormatDate;
        columnOrder = configs.producerDbPlainMetadataColumnOrder;
        tableName = configs.dbTable;
        metadataColumn = configs.producerDbPlainMetadataDate;
    }

    public boolean isEnabled() {
        return isNotBlank(metadataTopic);
    }

    public List<GenericRecord> sendMetadataRecords(List<GenericRecord> records) {
        if (isEnabled()) {
            var object = getLastRecord();
            if (object.isPresent()) {
                log.info("Fetched last record from metadata topic");
                var date = object.get().get(metadataDate).toString();
                var index = object.get().getInt(COUNT_FIELD);

                var filteredRecords = (ArrayList<GenericRecord>) filterRecordsByLastDate(records, date);
                records = new ArrayList<>(filteredRecords.subList(index, filteredRecords.size()));
            }
            sendMetadataRecords(records, object);
        }
        return records;
    }

    public String getQueryWithMetadataTopic() {
        var query = "";
        if (!columnOrder.isEmpty()) {
            var columns = columnOrder.split(",");
            query = "select * from " + tableName;
            if (columns.length > 0) {
                query = query + " order by " + metadataColumn + ",";
                for (var i = 0; i < columns.length; i++) {
                    query = query + columns[i] + ",";
                }
                query = query.substring(0, query.length() - 1);
            }
        }
        log.info("Query is " + query);
        return query;
    }

    private void sendMetadataRecords(List<GenericRecord> records, Optional<JSONObject> lastRecord) {
        var count = 0;
        var lastDate = "";
        if (lastRecord.isPresent()) {
            count = lastRecord.get().getInt(COUNT_FIELD);
            lastDate = lastRecord.get().get(metadataDate).toString();
        }
        KafkaProducer<String, GenericRecord> producer = producerFactory.newInstance();
        for (var record : records) {
            if (isEnabled()) {
                var builder = new SimpleRecord.Builder();
                var currentDate = record.get(metadataDate).toString();
                log.info("current date is " + currentDate);
                log.info("last date is " + lastDate);
                if (currentDate.equals(lastDate)) {
                    count++;
                } else {
                    count = 1;
                    lastDate = currentDate;
                }
                builder.setField(metadataDate, record.get(metadataDate).toString(), "");
                builder.setField(COUNT_FIELD, String.valueOf(count), "INT");
                var simpleRecord = SimpleRecord.clone(builder.build());
                var metadataSchema = simpleRecordManager.buildSchema(simpleRecord,
                        "Schema for metadata topic: " + metadataTopic, metadataTopic, "DATAHUB");
                var genericRecord = simpleRecord.getGenericRecord(metadataSchema);
                var producerMetadataRecord = new ProducerRecord<String, GenericRecord>(metadataTopic,
                        metadataRecordKey, genericRecord);
                log.debug("Sending record... \n" + producerMetadataRecord);
                producer.send(producerMetadataRecord, ((metadata, exception) -> {
                    if (exception != null) {
                        log.warn("Exception thrown while sending record: " + exception.getMessage());
                    }
                }));
            }
        }
    }

    private List<GenericRecord> filterRecordsByLastDate(List<GenericRecord> records, String lastDate) {
        var filteredRecords = new ArrayList<GenericRecord>();
        for (var record : records) {
            var obj = record.get(metadataDate);
            if (compareDates(lastDate, obj.toString()) <= 0) {
                filteredRecords.add(record);
            }
        }
        return filteredRecords;
    }

    private Integer compareDates(String date1, String date2) {
        try {
            var sdf = new SimpleDateFormat(metadataDateFormat);
            var recordDate = sdf.parse(date1);
            var metadataDate = sdf.parse(date2);
            return recordDate.compareTo(metadataDate);
        } catch (Exception e) {
            log.info("Date cannot be parsed " + e.toString());
        }
        return null;
    }

    private Optional<JSONObject> getLastRecord() {
        KafkaConsumer<String, GenericRecord> kafkaConsumer = consumerFactory.newInstance();
        var tp = new TopicPartition(metadataTopic, 0);
        kafkaConsumer.assign(Collections.singleton(tp));
        var lastRecordStartTime = System.currentTimeMillis();
        log.debug("Started looking for last record in metadata topic");
        while ((System.currentTimeMillis() - lastRecordStartTime) < 15000) {
            var records = kafkaConsumer.poll(Duration.ofMillis(100));
            var endPosition = kafkaConsumer.position(tp);
            kafkaConsumer.seek(tp, Math.max(0, endPosition - 1));  // last record unless the topic is empty
            // Only the last record is needed
            if (records.count() >= 1) {
                final var recordIterator = records.iterator();
                var record = recordIterator.next();
                while (recordIterator.hasNext()) {
                    record = recordIterator.next();
                }
                log.debug("The last record on metadata Topic " + record);
                var avroRecord = record.value();
                if (record.value() != null) {
                    var lastRecord = new JSONObject();
                    lastRecord.put(metadataDate, avroRecord.get(metadataDate));
                    lastRecord.put(COUNT_FIELD, avroRecord.get(COUNT_FIELD));
                    log.debug("The last record on metadata Topic after conversion " + lastRecord);
                    kafkaConsumer.close();
                    return Optional.of(lastRecord);
                }
            }
        }
        // if there are no records in the metadata topic
        log.debug("No record found on metadata topic");
        kafkaConsumer.close();
        return Optional.empty();
    }

}