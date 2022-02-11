package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.KafkaHelper;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.json.JsonService;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_SHAREDOC;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.FilenameGenerator.FILENAME_KAFKA_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MetadataProcessorTest extends AbstractKafkaBaseTest {
    private final ShareDocJsonService shareDocJsonService = new ShareDocJsonService(new JsonService());

    @Test
    void loadAndProduceMetadata() throws ExecutionException, InterruptedException {
        var kafkaClientHelper = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
        var topic = kafkaClientHelper.createRandomTopic();
        var fileName = "metadata/meta.json";
        var filenameGenerator = mock(FilenameGenerator.class);
        when(filenameGenerator.getMetadataFilePath(any())).thenReturn(fileName);

        var row = new Metadata.Row();
        row.setProperties(Map.of("a", 1));
        var metadata = new Metadata();
        metadata.setRows(List.of(row));
        var latestKey = "the_latest_key";

        var processor = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, "the_repo"))
                .withStringProducerFactory()
                .override(FilenameGenerator.class, filenameGenerator)
                .build().getInstance(MetadataProcessor.class);
        processor.loadAndProduceMetadata(metadata, latestKey);

        var records = kafkaClientHelper.consumeAllStringRecordsFromBeginning(topic);
        assertThat(records, hasSize(1));
        var record = records.get(0);
        KafkaHelper.assertStringConsumerRecord(record, nullValue(), equalTo("{\"a\":1}"), FILENAME_KAFKA_HEADER, fileName.getBytes());
    }

    @Test
    void noTopic() {
        var e = assertThrows(InvalidConfigurationException.class,
                () -> new MetadataProcessor(mock(FilenameGenerator.class), mock(ProducerFactory.class),
                        shareDocJsonService, mock(StatisticsAggregator.class), new Configs()));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'kafka.common.topic'. Current value: ''. Please check your configuration."));
    }
}