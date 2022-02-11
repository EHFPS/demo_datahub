package com.bayer.datahub.libs.kafka.consumer.bigquery;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.kafka.RecordService;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.json.JSONObject;

import javax.inject.Inject;
import java.time.Instant;
import java.time.ZoneId;
import java.util.LinkedList;
import java.util.List;

public class BqSltConsumer extends BigQueryConsumer {
    @Inject
    public BqSltConsumer(RecordConsumer recordConsumer, RecordService recordService, Configs configs) {
        super(recordConsumer, recordService, configs);
    }

    @Override
    protected void processRecordsAtLeastOnce(ConsumerRecords<?, ?> records) {
        var stringRecords = enrichRecords((ConsumerRecords<?, String>) records);
        gcsLoader.loadJson(stringRecords);
        bigQuery.loadJsonFromGcs(gcsLoader.getLastUploadedUri());
        recordConsumer.commit();
    }

    private List<String> enrichRecords(ConsumerRecords<?, String> records) {
        List<String> enrichedRecords = new LinkedList<>();
        for (var record : records) {
            var newRecord = new JSONObject(record.value());
            var key = record.key();
            var offset = record.offset();
            var timestamp = Instant.ofEpochMilli(record.timestamp()).atZone(ZoneId.systemDefault()).toLocalDateTime();
            var partition = record.partition();
            newRecord.put("key", key);
            newRecord.put("offset", offset);
            newRecord.put("partition", partition);
            newRecord.put("message_created_tsp", timestamp);
            enrichedRecords.add(newRecord.toString());
        }

        return enrichedRecords;
    }
}
