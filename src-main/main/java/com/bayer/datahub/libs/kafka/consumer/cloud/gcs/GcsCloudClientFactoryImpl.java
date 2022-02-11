package com.bayer.datahub.libs.kafka.consumer.cloud.gcs;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_GCS_GCS_PROJECT_ID_PROPERTY;

class GcsCloudClientFactoryImpl implements CloudClientFactory {
    private static final Logger log = LoggerFactory.getLogger(GcsCloudClientFactoryImpl.class);
    private final String projectId;
    private final String keyFile;
    private final String proxyHost;
    private final Integer proxyPort;
    private final String proxyUser;
    private final String proxyPassword;

    @Inject
    public GcsCloudClientFactoryImpl(Configs configs) {
        projectId = configs.consumerGcsGcsProjectId;
        keyFile = configs.consumerGcsGcsKeyFile;
        proxyHost = configs.consumerGcsGcsProxyHost;
        proxyPort = configs.consumerGcsGcsProxyPort;
        proxyUser = configs.consumerGcsGcsProxyUser;
        proxyPassword = configs.consumerGcsGcsProxyPassword;
    }

    @Override
    public Storage createClient() {
        log.debug("Creating Google Cloud Storage client...");
        try {
            if (!Strings.isNullOrEmpty(proxyHost)) {
                log.info("Work with proxy");
                System.setProperty("http.proxyHost", proxyHost);
                System.setProperty("http.proxyPort", String.valueOf(proxyPort));
                System.setProperty("http.proxyUser", proxyUser);
                System.setProperty("http.proxyPassword", proxyPassword);
                System.setProperty("https.proxyHost", proxyHost);
                System.setProperty("https.proxyPort", String.valueOf(proxyPort));
                System.setProperty("https.proxyUser", proxyUser);
                System.setProperty("https.proxyPassword", proxyPassword);
            } else {
                log.debug("Work without proxy");
            }
            if (Strings.isNullOrEmpty(projectId)) {
                throw new InvalidConfigurationException(CONSUMER_GCS_GCS_PROJECT_ID_PROPERTY, projectId);
            }
            var storage = StorageOptions.newBuilder()
                    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(keyFile)))
                    .setProjectId(projectId)
                    .build()
                    .getService();
            log.debug("Google Cloud Storage client is created for project '{}'", projectId);
            return storage;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
