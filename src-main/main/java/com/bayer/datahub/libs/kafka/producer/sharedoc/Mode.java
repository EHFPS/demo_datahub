package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import org.apache.commons.lang3.StringUtils;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_MODE_PROPERTY;

enum Mode {
    METADATA, CONTENT;

    public static Mode parse(String mode) {
        if (StringUtils.isBlank(mode)) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_MODE_PROPERTY, mode, Mode.values());
        }
        try {
            return Mode.valueOf(mode.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_MODE_PROPERTY, mode, Mode.values(), e);
        }
    }
}
