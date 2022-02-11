package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
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
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_BASE_URL_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_REPOSITORY_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocProducerModule.SHAREDOC_HTTP_CLIENT_NAME;
import static com.bayer.datahub.libs.utils.NetHelper.truncateEndingSlash;

class Authenticator {
    public static final String LOGIN_ENDPOINT_PATH = "/services/context/login";
    private static final Logger log = LoggerFactory.getLogger(Authenticator.class);
    private static final Pattern JSESSION_ID_PATTERN = Pattern.compile("^JSESSIONID=([\\w]*);.*$");
    private final HttpClient httpClient;
    private final String repository;
    private final String username;
    private final String password;
    private final String loginEndpoint;
    private String jSessionId;

    @Inject
    public Authenticator(@Named(SHAREDOC_HTTP_CLIENT_NAME) HttpClient httpClient, Configs configs) {
        this.httpClient = httpClient;

        repository = configs.producerSharedocRepository;
        if (StringUtils.isBlank(repository)) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, configs.producerSharedocRepository);
        }

        username = configs.producerSharedocUsername;
        password = configs.producerSharedocPassword;

        if (StringUtils.isBlank(configs.producerSharedocBaseUrl)) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_BASE_URL_PROPERTY, configs.producerSharedocBaseUrl);
        }
        var baseUrl = truncateEndingSlash(configs.producerSharedocBaseUrl);
        loginEndpoint = String.format("%s%s", baseUrl, LOGIN_ENDPOINT_PATH);
    }

    public String getJSessionId() {
        if (jSessionId == null) {
            jSessionId = loadJSessionId();
        }
        return jSessionId;
    }

    private String loadJSessionId() {
        try {
            log.debug("Authenticating in ShareDoc {}...", loginEndpoint);
            var boundary = "-------------" + UUID.randomUUID().toString();
            var request = HttpRequest.newBuilder()
                    .uri(URI.create(loginEndpoint))
                    .header("Content-Type", "multipart/form-data; boundary=" + boundary)
                    .POST(ofMultipartData(Map.of(
                            "repository", repository,
                            "username", username,
                            "password", password
                    ), boundary)).build();
            var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                var message = String.format("Authentication failed: endpoint=%s, responseCode=%s, errorMessage='%s'",
                        loginEndpoint, response.statusCode(), response.body());
                throw new RuntimeException(message);
            }
            var setCookieHeaderName = "Set-Cookie";
            var setCookieHeaderOpt = response.headers().firstValue(setCookieHeaderName);
            if (setCookieHeaderOpt.isEmpty()) {
                throw new RuntimeException("HTTP header '" + setCookieHeaderName + "' is absent");
            }
            String jSessionId = null;
            var matcher = JSESSION_ID_PATTERN.matcher(setCookieHeaderOpt.get());
            if (matcher.find()) {
                jSessionId = matcher.group(1);
            }
            if (StringUtils.isBlank(jSessionId)) {
                throw new RuntimeException("JSESSIONID is empty");
            }
            log.info("Authentication in ShareDoc is success");
            return jSessionId;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static HttpRequest.BodyPublisher ofMultipartData(Map<String, String> data, String boundary) {
        var sb = new StringBuilder();
        var separator = ("--" + boundary + "\r\nContent-Disposition: form-data; name=");
        for (var entry : data.entrySet()) {
            sb.append(separator);
            sb.append("\"").append(entry.getKey()).append("\"\r\n\r\n").append(entry.getValue()).append("\r\n");
        }
        sb.append("--").append(boundary).append("--");
        return HttpRequest.BodyPublishers.ofString(sb.toString());
    }
}
