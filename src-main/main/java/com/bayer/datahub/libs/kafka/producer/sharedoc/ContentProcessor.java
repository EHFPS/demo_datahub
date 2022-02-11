package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.chunk.ChunkService;
import com.bayer.datahub.libs.kafka.chunk.SyncAsyncProducer;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.bayer.datahub.libs.services.common.statistics.StatisticsTimer;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.ConcurrentInitializer;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_BASE_URL_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_REPOSITORY_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.FilenameGenerator.FILENAME_KAFKA_HEADER;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocLatestKeyConsumer.LATEST_KEY_KAFKA_HEADER;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocProducerModule.SHAREDOC_HTTP_CLIENT_NAME;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.OBJECT_ID_KEY;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.OUTGOING_BYTE_RATE_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.RECORD_RETRY_TOTAL_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.RECORD_SEND_TOTAL_METRIC_NAME;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.getLongProducerMetricValue;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.KAFKA_GROUP;
import static com.bayer.datahub.libs.utils.NetHelper.truncateEndingSlash;
import static org.apache.commons.lang3.StringUtils.isBlank;

class ContentProcessor {
    private static final Logger log = LoggerFactory.getLogger(ContentProcessor.class);
    private final String exportEndpoint;
    private final HttpClient httpClient;
    private final Authenticator authenticator;
    private final ChunkService<String, byte[]> chunkService;
    private final StatisticsAggregator statisticsAggregator;
    private final StatisticsTimer statisticsTimer;
    private final FilenameGenerator filenameGenerator;
    private final ProducerFactory producerFactory;
    private final String topic;
    private final ConcurrentInitializer<SyncAsyncProducer<String, byte[]>> producerInitializer =
            new LazyInitializer<>() {
                @Override
                protected SyncAsyncProducer<String, byte[]> initialize() {
                    var producer = new SyncAsyncProducer<String, byte[]>(producerFactory.newInstance(), statisticsAggregator);
                    log.debug("SyncAsyncProducer is instantiated.");
                    statisticsAggregator.incrementCounter("ContentProcessor-instantiated-SyncAsyncProducers");
                    return producer;
                }
            };

    @Inject
    public ContentProcessor(Authenticator authenticator, @Named(SHAREDOC_HTTP_CLIENT_NAME) HttpClient httpClient,
                            FilenameGenerator filenameGenerator,
                            @Named(MAIN_PRODUCER_FACTORY) ProducerFactory producerFactory,
                            ChunkService<String, byte[]> chunkService, StatisticsAggregator statisticsAggregator,
                            StatisticsTimer statisticsTimer, Configs configs) {
        this.authenticator = authenticator;
        this.chunkService = chunkService;
        this.statisticsAggregator = statisticsAggregator;
        this.statisticsTimer = statisticsTimer;
        if (isBlank(configs.producerSharedocBaseUrl)) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_BASE_URL_PROPERTY, configs.producerSharedocBaseUrl);
        }
        var baseUrl = truncateEndingSlash(configs.producerSharedocBaseUrl);
        var repository = configs.producerSharedocRepository;
        if (isBlank(repository)) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, configs.producerSharedocRepository);
        }
        exportEndpoint = String.format("%s/services/content/export/%s", baseUrl, repository);
        this.httpClient = httpClient;
        this.filenameGenerator = filenameGenerator;
        this.producerFactory = producerFactory;
        topic = configs.kafkaCommonTopic;
    }

    public void loadAndProduceContent(Metadata metadata, String lastKey) {
        log.debug("Creating HTTP requests for {} rows", metadata.getRows().size());
        var jSessionId = authenticator.getJSessionId();
        var producer = getSyncAsyncProducer();
        CompletableFuture.allOf(metadata.getRows().stream()
                .map(row -> sendHttpRequest(jSessionId, row)
                        .thenApply(this::checkHttpResponse)
                        .thenApply(response -> rowToRecord(row, lastKey, response.body(), topic))
                        .thenApply(chunkService::split)
                        .thenAccept(records -> produceChunkedRecords(producer, records))
                        .thenRun(() -> statisticsTimer.printStatisticsIfTime((data) -> statisticsAggregator
                                .title("Intermediate statistics #" + data.getCounter())
                                .addProducerMetrics(producer.metrics(), List.of(RECORD_SEND_TOTAL_METRIC_NAME,
                                        RECORD_RETRY_TOTAL_METRIC_NAME, OUTGOING_BYTE_RATE_METRIC_NAME))
                                .addMemoryUsage()
                                .getStatistics()))
                ).toArray(CompletableFuture[]::new)).join();
        getSyncAsyncProducer().flush();
        var metrics = getSyncAsyncProducer().metrics();
        getSyncAsyncProducer().close();
        log.info("Producing ShareDoc content is done");
        var producedRecords = getLongProducerMetricValue(metrics, RECORD_SEND_TOTAL_METRIC_NAME);
        statisticsAggregator
                .finalType()
                .title("Final statistics for " + topic)
                .addKeyValue(KAFKA_GROUP, "Produced records", producedRecords)
                .addMemoryUsage()
                .addProducerMetrics(metrics);
    }

    private HttpResponse<byte[]> checkHttpResponse(HttpResponse<byte[]> response) {
        statisticsAggregator.incrementCounter("ContentProcessor-http-responses-received");
        if (response.statusCode() >= 300) {
            throw new RuntimeException("Error response code: " + response.statusCode() +
                    " for " + response.uri());
        }
        return response;
    }

    private CompletableFuture<HttpResponse<byte[]>> sendHttpRequest(String jSessionId, Metadata.Row row) {
        return httpClient.sendAsync(createHttpRequest(jSessionId, row), BodyHandlers.ofByteArray());
    }

    private HttpRequest createHttpRequest(String jSessionId, Metadata.Row row) {
        var request = HttpRequest.newBuilder()
                .GET()
                .uri(URI.create(exportEndpoint + "/" + row.getProperties().get(OBJECT_ID_KEY)))
                .header("Cookie", "JSESSIONID=" + jSessionId)
                .build();
        statisticsAggregator.incrementCounter("ContentProcessor-http-request-sent");
        return request;
    }

    private ProducerRecord<String, byte[]> rowToRecord(Metadata.Row row, String lastDateHeaderValue, byte[] content, String topic) {
        String key = null;
        Header latestKeyHeader = new RecordHeader(LATEST_KEY_KAFKA_HEADER, lastDateHeaderValue.getBytes());
        var filename = filenameGenerator.getContentFilePath(row);
        Header filenameHeader = new RecordHeader(FILENAME_KAFKA_HEADER, filename.getBytes());
        Iterable<Header> headers = Arrays.asList(latestKeyHeader, filenameHeader);
        var partition = 0;
        var record = new ProducerRecord<>(topic, 0, key, content, headers);
        var recordStr = String.format("ProducerRecord<String, byte[]>(topic='%s', " +
                        "partition=%s, key='%s', value_length=%s, " +
                        "latest_key_header_name='%s', latest_key_header_value='%s', " +
                        "filename_header_name='%s', filename_header_value='%s')",
                topic, partition, key, content.length, LATEST_KEY_KAFKA_HEADER, lastDateHeaderValue,
                FILENAME_KAFKA_HEADER, filename);
        log.debug("Sending {}...", recordStr);
        statisticsAggregator.incrementCounter("ContentProcessor-row-to-record-conversions");
        return record;
    }

    private void produceChunkedRecords(SyncAsyncProducer<String, byte[]> producer,
                                       List<ProducerRecord<String, byte[]>> chunkedRecords) {
        var isChunked = chunkedRecords.size() > 1;
        if (!isChunked) {
            producer.sendAsync(chunkedRecords.get(0));
        } else {
            producer.sendSync(chunkedRecords);
        }
        statisticsAggregator.incrementCounter("ContentProcessor-produced-record-number");
    }

    private SyncAsyncProducer<String, byte[]> getSyncAsyncProducer() {
        try {
            return producerInitializer.get();
        } catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }
}
