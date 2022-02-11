package com.bayer.datahub.libs.kafka.consumer.bigquery;

import com.amazonaws.util.StringInputStream;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.Consumer;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.kafka.RecordService;
import com.bayer.datahub.libs.services.BigQuery.Client;
import com.bayer.datahub.libs.services.GCS.GcsLoader;
import com.google.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.kitesdk.data.spi.JsonUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BigQueryConsumer implements Consumer {
    private static final Logger log = LoggerFactory.getLogger(BigQueryConsumer.class);
    protected final Client bigQuery;
    protected final GcsLoader gcsLoader;
    protected final RecordConsumer recordConsumer;
    private final RecordService recordService;
    private final GcsLoadOption gcsLoadOption;
    private final Configs configs;
    private final BQStreamFormat streamFormat;

    @Inject
    public BigQueryConsumer(RecordConsumer recordConsumer, RecordService recordService, Configs configs) {
        this.recordConsumer = recordConsumer;
        this.recordService = recordService;
        bigQuery = new Client(configs.googleProjectId, configs.bigQueryDataset, configs.bigQueryTable);
        gcsLoader = new GcsLoader(configs.gcsBucketName, configs.gcsFolderName, configs.gcsFilePrefix);
        gcsLoadOption = GcsLoadOption.parse(configs.gcsLoadOption);
        streamFormat = BQStreamFormat.parse(configs.bqStreamFormat);
        this.configs = configs;
    }

    @Override
    public void init() {
        recordConsumer.init();
        bigQuery.init(configs);
        gcsLoader.init(configs);
    }

    @Override
    public void run() {
        recordConsumer.run(this::processRecordsAtLeastOnce, this::processRecordsExactlyOnce);
    }

    @Override
    public void stop() {
        recordConsumer.stop();
    }

    protected void processRecordsAtLeastOnce(ConsumerRecords<?, ?> records) {
        if (records.isEmpty()) {
            return;
        }

        switch (this.gcsLoadOption) {
            case AVRO:
                processAvroRecords(recordService.consumerRecordsToGenericRecords((ConsumerRecords<?, GenericRecord>) records));
                break;
            case JSON:
                processJsonRecords(recordService.consumerRecordsToStringArray((ConsumerRecords<?, String>) records));
                break;
            case NDJSON:
                processNewLineJsonRecords(recordService.consumerRecordsToStringArray((ConsumerRecords<?, String>) records));
                break;
            case NONE:
                processStreamingRecords(records, streamFormat);
        }

        recordConsumer.commit();
    }

    private void processRecordsExactlyOnce(ConsumerRecords<?, ?> records) {
        processRecordsAtLeastOnce(records);
    }

    private void processAvroRecords(List<GenericRecord> records) {
        try {
            log.info("Loading avro files into gcs...");
            this.gcsLoader.loadAvroFiles(records, records.get(0).getSchema());
        } catch (IOException e) {
            log.error("Error loading avro files to gcs...", e);
        }
        log.info(records.size() + " files loaded...");
        this.bigQuery.loadAvroFromGcs(gcsLoader.getLastUploadedUri());
    }

    private void processJsonRecords(List<String> records) {
        log.info("Loading json records into gcs...");
        gcsLoader.loadJson(records);
        bigQuery.loadJsonFromGcs(gcsLoader.getLastUploadedUri());
    }

    private void processNewLineJsonRecords(List<String> records) {
        log.info("Loading json records into gcs...");
        gcsLoader.loadNewLineJson(records);
        bigQuery.loadJsonFromGcs(gcsLoader.getLastUploadedUri());
    }

    private void processStreamingRecords(ConsumerRecords<?, ?> records, BQStreamFormat format) {
        log.info("Streaming insert of " + records.count() + " records");
        switch(format) {
            case AVRO:
                this.bigQuery.tableInsertRows(recordService.consumerRecordsToGenericRecords((ConsumerRecords<?, GenericRecord>)records));
                break;
            case JSON:
                List<String> stringRecords = recordService.consumerRecordsToStringArray((ConsumerRecords<?, String>) records);
                try {
                    this.bigQuery.tableInsertRows(buildGenericRecords(stringRecords));
                } catch (IOException e) {
                    throw new RuntimeException("Error processing json records", e);
                }
                break;
        }
    }

    private List<GenericRecord> buildGenericRecords(List<String> records) throws IOException {
        List<GenericRecord> genericRecords = new ArrayList<>();
        Schema schema = JsonUtil.inferSchema(new StringInputStream(String.join("", records)),
                recordConsumer.getTopic(), records.size());
        log.debug("Schema inferred " + schema.toString());
        for(String record : records) {
            genericRecords.add(jsonToAvro(record, schema));
        }
        return genericRecords;
    }

    private Schema getSchema(String record) {
        return JsonUtil.inferSchema(JsonUtil.parse(record), recordConsumer.getTopic());
    }

    private GenericRecord jsonToAvro(String jsonString, Schema schema) throws IOException {
        JsonAvroConverter converter = new JsonAvroConverter();
        DatumReader<GenericRecord> reader1 = new GenericDatumReader<>(schema);
        Decoder decoder1 = DecoderFactory.get().binaryDecoder(converter.convertToAvro(jsonString.getBytes(), schema), null);
        return reader1.read(null, decoder1);
    }

}
