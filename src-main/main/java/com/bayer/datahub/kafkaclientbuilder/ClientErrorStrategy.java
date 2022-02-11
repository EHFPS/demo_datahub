package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;

import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_ERROR_STRATEGY_PROPERTY;

public enum ClientErrorStrategy {
    SKIP_ALL_CLIENTS,
    SKIP_CURRENT_CLIENT;

    public static ClientErrorStrategy parse(String clientTypeStr) {
        try {
            return valueOf(clientTypeStr.toUpperCase());
        } catch (Exception e) {
            throw new InvalidConfigurationException(CLIENT_ERROR_STRATEGY_PROPERTY, clientTypeStr, values(), e);
        }
    }
}
