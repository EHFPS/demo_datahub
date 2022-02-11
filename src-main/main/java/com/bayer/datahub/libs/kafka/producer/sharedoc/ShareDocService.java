package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.Objects;

import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocProducerModule.SHAREDOC_HTTP_CLIENT_NAME;
import static com.bayer.datahub.libs.utils.NetHelper.truncateEndingSlash;

class ShareDocService {
    public static final String OBJECT_ID_KEY = "r_object_id";
    public static final String MODIFY_DATE_KEY = "r_modify_date";
    public static final String STUDY_NAME_KEY = "study_number";
    private static final Logger log = LoggerFactory.getLogger(ShareDocService.class);
    private static final String LAST_PROCESSED_DATE_PLACEHOLDER = "\\{\\{lastProcessedDate}}";
    private static final String LAST_MODIFIED_DATE_DEFAULT = "01/01/1900 00:00:00";
    private static final DateTimeFormatter QUERY_MODIFY_DATE_FORMATTER = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");
    private static final DateTimeFormatter RESPONSE_MODIFY_DATE_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    private final ShareDocJsonService shareDocJsonService;
    private final ShareDocLatestKeyConsumer shareDocLatestKeyConsumer;
    private final String query;
    private final String getQueryResultEndpoint;
    private final HttpClient httpClient;
    private final Authenticator authenticator;

    @Inject
    public ShareDocService(ShareDocJsonService shareDocJsonService, ShareDocLatestKeyConsumer shareDocLatestKeyConsumer,
                           Authenticator authenticator, @Named(SHAREDOC_HTTP_CLIENT_NAME) HttpClient httpClient,
                           Configs configs) {
        this.shareDocJsonService = shareDocJsonService;
        this.shareDocLatestKeyConsumer = shareDocLatestKeyConsumer;
        this.query = configs.producerSharedocQuery;
        this.authenticator = authenticator;
        var baseUrl = truncateEndingSlash(configs.producerSharedocBaseUrl);
        var repository = configs.producerSharedocRepository;
        getQueryResultEndpoint = String.format("%s/services/tools/getqueryresults/%s", baseUrl, repository);
        this.httpClient = httpClient;
    }

    public Metadata loadMetadata() {
        try {
            var jSessionId = authenticator.getJSessionId();
            var query = fillQuery();
            var body = String.format("{\"query\": \"%s\"}", query);
            log.debug("Body: {}", body);
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(getQueryResultEndpoint))
                    .POST(HttpRequest.BodyPublishers.ofString(body))
                    .header("Content-Type", "application/json")
                    .header("Cookie", "JSESSIONID=" + jSessionId)
                    .build();
            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            var metadataBinary = response.body();
            return shareDocJsonService.deserializeMetadata(metadataBinary);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private String fillQuery() {
        var latestKey = shareDocLatestKeyConsumer.getLatestKey();
        if (StringUtils.isBlank(latestKey)) {
            latestKey = LAST_MODIFIED_DATE_DEFAULT;
        }
        return query.replaceAll(LAST_PROCESSED_DATE_PLACEHOLDER, latestKey);
    }

    public String findLastModifyDate(Metadata metadata) {
        var lastModifiedDate = metadata.getRows().stream()
                .map(Metadata.Row::getProperties)
                .map(properties -> properties.get(MODIFY_DATE_KEY))
                .filter(Objects::nonNull)
                .map(modifiedDate -> (String) modifiedDate)
                .map(RESPONSE_MODIFY_DATE_FORMATTER::parse)
                .map(Instant::from)
                .max(Comparator.naturalOrder())
                .map(instant -> LocalDateTime.ofInstant(instant, ZoneId.systemDefault()))
                .map(QUERY_MODIFY_DATE_FORMATTER::format)
                .orElse(LAST_MODIFIED_DATE_DEFAULT);
        log.debug("Last modified date in retrieved metadata: {}", lastModifiedDate);
        return lastModifiedDate;
    }
}
