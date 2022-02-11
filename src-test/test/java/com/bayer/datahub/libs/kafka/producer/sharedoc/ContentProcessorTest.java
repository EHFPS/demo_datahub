package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.WebMockServerBaseTest;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.chunk.ChunkService;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.common.statistics.StatisticsTimer;
import okhttp3.mockwebserver.MockResponse;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;

import static com.bayer.datahub.KafkaHelper.assertStringConsumerRecord;
import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_SHAREDOC;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.FilenameGenerator.FILENAME_KAFKA_HEADER;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.OBJECT_ID_KEY;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ContentProcessorTest extends WebMockServerBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(AbstractKafkaBaseTest.getBrokers(), AbstractKafkaBaseTest.getSchemaRegistryUrl());

    @Test
    void loadAndProduceContent() {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var body = "body content";
        var path = "services/content/export/the_repo/the_obj_id";
        addPathResponse(path, body);

        var latestKey = "the_latest_key";

        var authenticator = createAuthenticator();

        var objectId = "the_obj_id";
        var row = new Metadata.Row();
        row.setProperties(Map.of(OBJECT_ID_KEY, objectId));
        var rows = List.of(row);
        var metadata = new Metadata();
        metadata.setRows(rows);

        var fileName = "metadata/meta.json";
        var filenameGenerator = mock(FilenameGenerator.class);
        when(filenameGenerator.getContentFilePath(eq(row))).thenReturn(fileName);

        var processor = FactoryBuilder.newBuilder(createConfigMap(topic))
                .withByteArrayProducerFactory()
                .override(Authenticator.class, authenticator)
                .override(FilenameGenerator.class, filenameGenerator)
                .build().getInstance(ContentProcessor.class);
        processor.loadAndProduceContent(metadata, latestKey);

        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(1));
        var actRecord1 = actRecords.get(0);
        assertStringConsumerRecord(actRecord1, nullValue(), equalTo(body), FILENAME_KAFKA_HEADER, fileName.getBytes());
    }

    @Test
    void loadAndProduceContentError() {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var path = "services/content/export/the_repo/the_obj_id";
        var errorCode = 500;
        addPathResponse(path, new MockResponse().setResponseCode(errorCode));

        var latestKey = "the_latest_key";

        var authenticator = createAuthenticator();

        var objectId = "the_obj_id";
        var row = new Metadata.Row();
        row.setProperties(Map.of(OBJECT_ID_KEY, objectId));
        var rows = List.of(row);
        var metadata = new Metadata();
        metadata.setRows(rows);

        var fileName = "metadata/meta.json";
        var filenameGenerator = mock(FilenameGenerator.class);
        when(filenameGenerator.getContentFilePath(eq(row))).thenReturn(fileName);

        var processor = FactoryBuilder.newBuilder(createConfigMap(topic))
                .withByteArrayProducerFactory()
                .override(Authenticator.class, authenticator)
                .override(FilenameGenerator.class, filenameGenerator)
                .build().getInstance(ContentProcessor.class);
        var e = assertThrows(CompletionException.class, () -> processor.loadAndProduceContent(metadata, latestKey));
        var expMessage = format("java.lang.RuntimeException: Error response code: %d for " +
                        "http://localhost:%d/services/content/export/the_repo/the_obj_id",
                errorCode, server.getPort());
        assertThat(e.getMessage(), equalTo(expMessage));
        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(0));
    }

    private Authenticator createAuthenticator() {
        var authenticator = mock(Authenticator.class);
        var jSessionId = "abcd";
        when(authenticator.getJSessionId()).thenReturn(jSessionId);
        return authenticator;
    }

    private Map<String, String> createConfigMap(String topic) {
        return Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_MODE_PROPERTY, Mode.CONTENT.name(),
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, "the_repo");
    }

    @Test
    void noBaseUrl() {
        var e = assertThrows(InvalidConfigurationException.class,
                () -> new ContentProcessor(mock(Authenticator.class), mock(HttpClient.class), mock(FilenameGenerator.class),
                        mock(ProducerFactory.class), mock(ChunkService.class), mock(StatisticsAggregator.class),
                        mock(StatisticsTimer.class), new Configs()));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.sharedoc.base.url'. Current value: ''. Please check your configuration."));
    }

    @Test
    void noRepository() {
        var configs = new Configs(Map.of(PRODUCER_SHAREDOC_BASE_URL_PROPERTY, "the_base_url"));
        var e = assertThrows(InvalidConfigurationException.class,
                () -> new ContentProcessor(mock(Authenticator.class), mock(HttpClient.class), mock(FilenameGenerator.class),
                        mock(ProducerFactory.class), mock(ChunkService.class), mock(StatisticsAggregator.class),
                        mock(StatisticsTimer.class), configs));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.sharedoc.repository'. Current value: ''. Please check your configuration."));
    }
}