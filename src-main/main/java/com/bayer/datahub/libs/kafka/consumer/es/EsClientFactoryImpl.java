package com.bayer.datahub.libs.kafka.consumer.es;

import com.bayer.datahub.libs.config.Configs;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.ConcurrentInitializer;
import org.apache.commons.lang3.concurrent.LazyInitializer;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.net.ssl.SSLContext;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyStore;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_ES_CONNECTION_TRUSTSTORE_PATH_PROPERTY;

class EsClientFactoryImpl implements EsClientFactory {
    private static final Logger log = LoggerFactory.getLogger(EsClientFactoryImpl.class);
    private final String address;
    private final String username;
    private final String password;
    private final String truststorePath;
    private final String truststorePassword;
    private final ConcurrentInitializer<RestClient> initializer = new LazyInitializer<>() {
        @Override
        protected RestClient initialize() {
            return createEsClient();
        }
    };

    @Inject
    public EsClientFactoryImpl(Configs configs) {
        address = configs.consumerEsConnectionAddress;
        username = configs.consumerEsConnectionUsername;
        password = configs.consumerEsConnectionPassword;
        truststorePath = configs.consumerEsConnectionTruststorePath;
        truststorePassword = configs.consumerEsConnectionTruststorePassword;
    }

    @Override
    public RestClient getRestClient() {
        try {
            return initializer.get();
        } catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    private RestClient createEsClient() {
        log.debug("Initializing RestHighLevelClient...");
        var sslContext = createSslContext();
        var credentialsProvider = new BasicCredentialsProvider();
        var credentials = new UsernamePasswordCredentials(username, password);
        credentialsProvider.setCredentials(AuthScope.ANY, credentials);
        var httpHost = HttpHost.create(address);
        return RestClient.builder(httpHost)
                .setHttpClientConfigCallback(builder -> builder
                        .setDefaultCredentialsProvider(credentialsProvider)
                        .setSSLContext(sslContext))
                .build();
    }

    private SSLContext createSslContext() {
        try {
            SSLContext sslContext;
            if (StringUtils.isNotBlank(truststorePath)) {
                log.debug("Loading truststore '{}'...", truststorePath);
                var trustStorePath = Paths.get(truststorePath);
                var truststore = KeyStore.getInstance("pkcs12");
                try (var is = Files.newInputStream(trustStorePath)) {
                    truststore.load(is, truststorePassword.toCharArray());
                }
                sslContext = SSLContexts.custom().loadTrustMaterial(truststore, null).build();
                log.debug("Truststore is loaded: '{}'", truststorePath);
            } else {
                log.debug("Truststore is not specified ('{}')", CONSUMER_ES_CONNECTION_TRUSTSTORE_PATH_PROPERTY);
                sslContext = null;
            }
            return sslContext;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
