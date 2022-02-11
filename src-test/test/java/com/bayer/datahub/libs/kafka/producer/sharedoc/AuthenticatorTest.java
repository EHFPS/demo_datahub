package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.WebMockServerBaseTest;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import okhttp3.mockwebserver.MockResponse;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_SHAREDOC;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.Authenticator.LOGIN_ENDPOINT_PATH;
import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class AuthenticatorTest extends WebMockServerBaseTest {
    private static final String REPOSITORY = "the_repo";

    @Test
    void getJSessionId() throws InterruptedException {
        var expSessionId = "38117334AB3CA361D4756A3ABC0BF2AE";
        var expUsername = "the_user";
        var expPassword = "the_password";

        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", format("JSESSIONID=%s; Path=/cara-rest/; Secure; HttpOnly", expSessionId));
        var authenticator = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, REPOSITORY,
                PRODUCER_SHAREDOC_USERNAME_PROPERTY, expUsername,
                PRODUCER_SHAREDOC_PASSWORD_PROPERTY, expPassword))
                .build().getInstance(Authenticator.class);
        var actSessionId = authenticator.getJSessionId();
        assertThat(actSessionId, equalTo(expSessionId));
        var request = server.takeRequest();
        var actBody = request.getBody().readUtf8();
        assertThat(actBody, allOf(containsString(REPOSITORY), containsString(expUsername), containsString(expPassword)));
    }

    @Test
    void getJSessionIdCookieIsAbsent() {
        addPathResponse(LOGIN_ENDPOINT_PATH);
        var authenticator = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, REPOSITORY))
                .build().getInstance(Authenticator.class);

        var e = assertThrows(RuntimeException.class, authenticator::getJSessionId);
        assertThat(e.getMessage(), equalTo("HTTP header 'Set-Cookie' is absent"));
    }

    @Test
    void getJSessionIdNot200() {
        addPathResponse(LOGIN_ENDPOINT_PATH, new MockResponse().setResponseCode(403).setBody("The error cause"));
        var authenticator = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, REPOSITORY))
                .build().getInstance(Authenticator.class);
        var e = assertThrows(RuntimeException.class, authenticator::getJSessionId);
        var expMessage = format("Authentication failed: endpoint=%s%s, responseCode=403, errorMessage='The error cause'",
                baseUrl, LOGIN_ENDPOINT_PATH.substring(1));
        assertThat(e.getMessage(), equalTo(expMessage));
    }

    @Test
    void getJSessionIdEmpty() {
        var expSessionId = "";
        addPathResponse(LOGIN_ENDPOINT_PATH, "Set-Cookie", format("JSESSIONID=%s; Path=/cara-rest/; Secure; HttpOnly", expSessionId));
        var authenticator = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
                PRODUCER_SHAREDOC_BASE_URL_PROPERTY, baseUrl,
                PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, REPOSITORY))
                .build().getInstance(Authenticator.class);
        var e = assertThrows(RuntimeException.class, authenticator::getJSessionId);
        assertThat(e.getMessage(), equalTo("JSESSIONID is empty"));
    }

    @Test
    void getJSessionIdNoBaseUrl() {
        var configs = new Configs(Map.of(PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, "the_repo"));
        var e = assertThrows(InvalidConfigurationException.class,
                () -> new Authenticator(mock(HttpClient.class), configs));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.sharedoc.base.url'. Current value: ''. Please check your configuration."));
    }

    @Test
    void getJSessionIdNoRepository() {
        var configs = new Configs(Map.of(PRODUCER_SHAREDOC_BASE_URL_PROPERTY, "http://sharedoc.com"));
        var e = assertThrows(InvalidConfigurationException.class,
                () -> new Authenticator(mock(HttpClient.class), configs));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.sharedoc.repository'. Current value: ''. Please check your configuration."));
    }
}