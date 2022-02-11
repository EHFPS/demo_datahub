package com.bayer.datahub.libs.kafka.consumer.db;

import com.bayer.datahub.libs.interfaces.Consumer;
import com.bayer.datahub.libs.interfaces.DbContext;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.kafka.RecordService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;

/**
 * DbConsumer reads messages from Avro topic and saves its to a database table.
 */
class DbConsumer implements Consumer {
    private static final Logger log = LoggerFactory.getLogger(DbConsumer.class);
    private final DbContext context;
    private final RecordService recordService;
    private final RecordConsumer recordConsumer;

    @Inject
    public DbConsumer(RecordConsumer recordConsumer, DbContext context, RecordService recordService) {
        this.recordConsumer = recordConsumer;
        this.context = context;
        this.recordService = recordService;
    }

    @Override
    public void init() {
        recordConsumer.init();
        context.connect();
    }

    @Override
    public void run() {
        recordConsumer.run((records -> {
            if (records.isEmpty()) {
                return;
            }
            log.info("Saving records to database...");
            var genericRecords = recordService.consumerRecordsToGenericRecords((ConsumerRecords<?, GenericRecord>) records);
            context.saveRecords(genericRecords);
            recordConsumer.commit();
            log.info("Records saved: {} records", genericRecords.size());
        }));
    }

    @Override
    public void stop() {
        recordConsumer.stop();
        context.close();
    }
}
