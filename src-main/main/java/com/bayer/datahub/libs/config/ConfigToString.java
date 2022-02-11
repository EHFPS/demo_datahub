package com.bayer.datahub.libs.config;

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_S3_AWS_AUTH_BASIC_ACCESS_KEY_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_S3_AWS_AUTH_BASIC_SECRET_KEY_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_S3_AWS_AUTH_ROLE_SESSION_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_SPLUNK_HEC_KEYSTORE_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.DB_PASSWORD;
import static com.bayer.datahub.libs.config.PropertyNames.DB_USER;
import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_SSL_KEYSTORE_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_SSL_KEY_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.KAFKA_COMMON_SSL_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.NOTIFICATION_KAFKA_KEYSTORE_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.NOTIFICATION_KAFKA_KEY_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.NOTIFICATION_KAFKA_TRUSTSTORE_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_PASSWORD_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_USERNAME_PROPERTY;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_KEY_PASSWORD_CONFIG;
import static org.apache.kafka.common.config.SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG;

/**
 * Return string representation of all properties, but hide passwords.
 */
public class ConfigToString {
    private static final List<String> SECRET_PROPERTIES = Arrays.asList(
            KAFKA_COMMON_SSL_KEY_PASSWORD_PROPERTY,
            KAFKA_COMMON_SSL_TRUSTSTORE_PASSWORD_PROPERTY,
            KAFKA_COMMON_SSL_KEYSTORE_PASSWORD_PROPERTY,
            PRODUCER_SHAREDOC_USERNAME_PROPERTY,
            PRODUCER_SHAREDOC_PASSWORD_PROPERTY,
            CONSUMER_S3_AWS_AUTH_BASIC_ACCESS_KEY_PROPERTY,
            CONSUMER_S3_AWS_AUTH_BASIC_SECRET_KEY_PROPERTY,
            CONSUMER_S3_AWS_AUTH_ROLE_SESSION_PROPERTY,
            CONSUMER_SPLUNK_HEC_KEYSTORE_PASSWORD_PROPERTY,
            DB_USER,
            DB_PASSWORD,
            SSL_KEY_PASSWORD_CONFIG,
            SSL_KEYSTORE_PASSWORD_CONFIG,
            SSL_TRUSTSTORE_PASSWORD_CONFIG,
            NOTIFICATION_KAFKA_TRUSTSTORE_PASSWORD_PROPERTY,
            NOTIFICATION_KAFKA_KEYSTORE_PASSWORD_PROPERTY,
            NOTIFICATION_KAFKA_KEY_PASSWORD_PROPERTY
    );

    public static String secureToString(String title, Map<String, Object> properties) {
        var basePropertiesStr = mapToString(properties);
        return String.format("%s\n%s\n", title, basePropertiesStr);
    }

    private static String mapToString(Map<String, Object> properties) {
        return properties.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .map(entry -> {
                    var propertyName = entry.getKey();
                    String propertyValue;
                    if (!SECRET_PROPERTIES.contains(propertyName)) {
                        propertyValue = entry.getValue() != null ? entry.getValue().toString() : "null";
                    } else {
                        propertyValue = secure(entry.getValue());
                    }
                    return String.format("    %s=%s", propertyName, propertyValue);
                }).collect(Collectors.joining("\n"));
    }

    private static String secure(Object value) {
        if (value == null) {
            return "<null>";
        } else if (StringUtils.isBlank(value.toString())) {
            return "<empty>";
        } else {
            return "<hidden_not_empty>";
        }
    }
}