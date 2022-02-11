package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.WebMockServerBaseTest;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.services.common.statistics.Statistics;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.ResourceHelper.resourceToBytes;
import static com.bayer.datahub.ResourceHelper.resourceToString;
import static com.bayer.datahub.libs.StatisticsAssert.assertStatistics;
import static com.bayer.datahub.libs.StatisticsAssert.sortedMapOf;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.Authenticator.LOGIN_ENDPOINT_PATH;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.FilenameGenerator.FILENAME_KAFKA_HEADER;
import static com.bayer.datahub.libs.services.common.statistics.Statistics.Group.groupOf;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.*;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class ShareDocProducerTest extends WebMockServerBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(AbstractKafkaBaseTest.getBrokers(), AbstractKafkaBaseTest.getSchemaRegistryUrl());
    private static final String JSESSIONID_COOKIE = format("JSESSIONID=%s; Path=/cara-rest/; Secure; HttpOnly",
            "38117334AB3CA361D4756A3ABC0BF2AE");
    private final String topic = KAFKA_CLIENT_HELPER.createRandomTopic();

    @Test
    void metadataFlow() {
        var metadataBody = resourceToBytes(ShareDocProducerTest.class, "ShareDocProducerTest_metadataFlow_in.json");

        var path2 = "services/tools/getqueryresults/the_repo";
        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", JSESSIONID_COOKIE);
        addPathResponse(path2, metadataBody);

        var contentProcessor = mock(ContentProcessor.class);
        var factory = FactoryBuilder.newBuilder(createConfigMap(Mode.METADATA))
                .withStringProducerFactory()
                .withStringConsumerFactory()
                .override(ContentProcessor.class, contentProcessor)
                .build();
        var producer = factory.getKafkaClient();
        producer.init();
        producer.run();
        producer.stop();

        verify(contentProcessor, times(0)).loadAndProduceContent(any(), any());

        var expContent1 = resourceToString(ShareDocProducerTest.class, "ShareDocProducerTest_metadataFlow_out_1.json");
        var expContent2 = resourceToString(ShareDocProducerTest.class, "ShareDocProducerTest_metadataFlow_out_2.json");

        assertTopic(List.of(expContent1, expContent2), List.of(
                "metadata-the_repo/090192f2802548b4-T103130-8.json",
                "metadata-the_repo/090192f28025489d-T103112-8.json"
        ), true);

        var statistics = factory.getInstance(StatisticsAggregator.class).getStatistics();
        assertStatistics(
                Statistics.Type.FINAL,
                statistics,
                "Final statistics for " + topic,
                " ",
                null,
                Statistics.Status.SUCCESS,
                List.of(groupOf(KAFKA_GROUP, sortedMapOf("Produced records", 2L)),
                        groupOf(COUNTER_GROUP, sortedMapOf(
                                "MetadataProcessor-sent-records", 2,
                                "Received-rows-from-ShareDoc", 2)),
                        groupOf(PRODUCER_METRICS_GROUP, sortedMapOf(
                                "record_send_total", 2.0,
                                "record_retry_total", 0.0
                        ))));
    }

    @Test
    void contentFlow() throws ExecutionException, InterruptedException {
        var metadataBody = resourceToString(ShareDocProducerTest.class, "ShareDocProducerTest_contentFlow.json");
        var contentStr = "the content body";
        var contentBody = contentStr.getBytes();

        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", JSESSIONID_COOKIE);
        addPathResponse("/services/tools/getqueryresults/the_repo", metadataBody);
        addPathResponse("services/content/export/the_repo/090192f280253493", contentBody);
        addPathResponse("services/content/export/the_repo/090192f28025489d", contentBody);
        addPathResponse("services/content/export/the_repo/090192f280253475", contentBody);
        addPathResponse("services/content/export/the_repo/090192f2802534d7", contentBody);
        addPathResponse("services/content/export/the_repo/090192f2802548b4", contentBody);
        addPathResponse("services/content/export/the_repo/090192f2802534bc", contentBody);

        var metadataProcessor = mock(MetadataProcessor.class);
        var factory = FactoryBuilder.newBuilder(createConfigMap(Mode.CONTENT))
                .withByteArrayProducerFactory()
                .withStringConsumerFactory()
                .override(MetadataProcessor.class, metadataProcessor)
                .build();
        var producer = factory.getKafkaClient();
        producer.init();
        producer.run();
        producer.stop();

        verify(metadataProcessor, times(0)).loadAndProduceMetadata(any(), any());

        var expFilenames = asList(
                "files-the_repo/090192f2802534bc-T103529-0.pdf",
                "files-the_repo/090192f28025489d-T103112-8.pdf",
                "files-the_repo/090192f2802534d7-T103563-8.pdf",
                "files-the_repo/090192f2802548b4-T103130-8.pdf",
                "files-the_repo/090192f280253493-T103290-5.pdf",
                "files-the_repo/090192f280253475-T103925-0.pdf");
        assertTopic(List.of(contentStr, contentStr, contentStr, contentStr, contentStr, contentStr), expFilenames, false);

        var statistics = factory.getInstance(StatisticsAggregator.class).getStatistics();
        assertStatistics(
                Statistics.Type.FINAL,
                statistics,
                "Final statistics for " + topic,
                " ",
                null,
                Statistics.Status.SUCCESS,
                List.of(groupOf(KAFKA_GROUP, sortedMapOf("Produced records", 6L)),
                        groupOf(COUNTER_GROUP, sortedMapOf(
                                "ChunkService-records-not-split-to-chunks", 6,
                                "ContentProcessor-http-request-sent", 6,
                                "ContentProcessor-http-responses-received", 6,
                                "ContentProcessor-instantiated-SyncAsyncProducers", 1,
                                "ContentProcessor-produced-record-number", 6,
                                "ContentProcessor-row-to-record-conversions", 6,
                                "Received-rows-from-ShareDoc", 6,
                                "SyncAsyncProducer-received-for-sending-async", 6,
                                "SyncAsyncProducer-sent-async", 6)),
                        groupOf(MEMORY_GROUP, sortedMapOf()),
                        groupOf(PRODUCER_METRICS_GROUP, sortedMapOf(
                                "record_send_total", 6.0,
                                "record_retry_total", 0.0
                        ))));
    }

    private Map<String, String> createConfigMap(Mode mode) {
        return Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                KAFKA_COMMON_TOPIC_PROPERTY, topic,
                PRODUCER_SHAREDOC_MODE_PROPERTY, mode.name(),
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, "the_repo",
                PRODUCER_SHAREDOC_USERNAME_PROPERTY, "the_username",
                PRODUCER_SHAREDOC_PASSWORD_PROPERTY, "the_property");
    }

    private void assertTopic(List<String> bodyContentList, List<String> expFilenames, boolean jsonBodyContent) {
        var records = KAFKA_CLIENT_HELPER.consumeAllBinaryRecordsFromBeginning(topic);
        assertThat(records, hasSize(expFilenames.size()));
        var record = records.get(0);
        assertThat(record.key(), nullValue());
        var headers = record.headers();
        var filenameHeader = headers.lastHeader(FILENAME_KAFKA_HEADER);
        assertNotNull(filenameHeader);
        assertThat(filenameHeader.key(), equalTo(FILENAME_KAFKA_HEADER));

        var actKeys = records.stream()
                .map(r -> new String(r.headers().lastHeader(FILENAME_KAFKA_HEADER).value()))
                .collect(toList());
        assertThat(actKeys, containsInAnyOrder(expFilenames.toArray()));

        var actValues = records.stream().map(r -> new String(r.value())).collect(toList());
        if (jsonBodyContent) {
            for (int i = 0; i < bodyContentList.size(); i++) {
                String expContent = bodyContentList.get(i);
                String actContent = actValues.get(i);
                JSONAssert.assertEquals(actContent, expContent, false);
            }
        } else {
            assertThat(actValues, containsInAnyOrder(bodyContentList.toArray()));
        }
    }
}