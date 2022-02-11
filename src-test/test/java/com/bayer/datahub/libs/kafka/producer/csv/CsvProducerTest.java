package com.bayer.datahub.libs.kafka.producer.csv;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.Statistics;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.fileio.CsvRecordDelimiter;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.bayer.datahub.ReflectionHelper.readObjectField;
import static com.bayer.datahub.ResourceHelper.resourceToAbsolutePath;
import static com.bayer.datahub.libs.StatisticsAssert.assertStatistics;
import static com.bayer.datahub.libs.StatisticsAssert.sortedMapOf;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static com.bayer.datahub.libs.services.common.statistics.Statistics.Group.groupOf;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.MEMORY_GROUP;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.PRODUCER_METRICS_GROUP;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CsvProducerTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());

    @Test
    void run() {
        var file = resourceToAbsolutePath(CsvProducerTest.class, "CsvProducerTest.csv");
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();

        var fieldNameA = "fieldA";
        var fieldNameB = "fieldB";
        var schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name(fieldNameA).type().stringType().noDefault()
                .name(fieldNameB).type().stringType().noDefault()
                .endRecord();
        var factory = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_CSV.name(),
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_CSV_FILE_PATH_PROPERTY, file,
                PRODUCER_CSV_VALUE_DELIMITER_PROPERTY, ",",
                PRODUCER_CSV_RECORD_DELIMITER_PROPERTY, CsvRecordDelimiter.LF.name()))
                .withAvroProducerFactory()
                .withSchemaRegistryMock(schema)
                .build();
        var producer = factory.getKafkaClient();
        producer.init();
        producer.run();
        producer.stop();

        verify(factory.getInstance(ProducerFactory.class, MAIN_PRODUCER_FACTORY)).newInstance();
        KafkaProducer<String, GenericRecord> kafkaProducer = readObjectField(producer, "producer");
        verify(kafkaProducer, times(2)).send(any(ProducerRecord.class), any(Callback.class));

        var statistics = factory.getInstance(StatisticsAggregator.class).getStatistics();
        assertStatistics(
                Statistics.Type.FINAL,
                statistics,
                "Final statistics for " + topic,
                " ",
                null,
                Statistics.Status.SUCCESS,
                List.of(groupOf(MEMORY_GROUP, sortedMapOf()),
                        groupOf(PRODUCER_METRICS_GROUP, sortedMapOf(
                                "record_send_total", 2.0,
                                "record_retry_total", 0.0
                        ))));
    }
}