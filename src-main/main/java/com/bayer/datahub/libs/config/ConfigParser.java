package com.bayer.datahub.libs.config;

import com.typesafe.config.Config;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

class ConfigParser {
    private static final int DEFAULT_INT = -1;
    private static final long DEFAULT_LONG = -1L;
    private static final boolean DEFAULT_BOOLEAN = false;
    private static final char DEFAULT_CHAR = ' ';
    private final Map<String, Object> allProperties = new HashMap<>();
    private final Config config;

    public ConfigParser(Config config) {
        this.config = config;
    }

    public enum ConfigTypes {
        STRING, INT, LONG, BOOLEAN, CHAR
    }

    public <T> T get(String variable, ConfigTypes type, T defaultValue) {
        if (!isAbsentOrEmpty(variable)) {
            return get(variable, type);
        } else {
            allProperties.put(variable, defaultValue);
            return defaultValue;
        }
    }

    private boolean isAbsentOrEmpty(String variable) {
        return !config.hasPath(variable) || isEmpty(config.getString(variable));
    }

    public <T> T get(String variable, ConfigTypes type) {
        Object value = "";

        if (config.hasPath(variable) && isNotEmpty(config.getAnyRef(variable).toString())) {
            switch (type) {
                case STRING:
                    value = config.getString(variable);
                    break;
                case INT:
                    value = config.getInt(variable);
                    break;
                case LONG:
                    value = config.getLong(variable);
                    break;
                case BOOLEAN:
                    value = config.getBoolean(variable);
                    break;
                case CHAR:
                    value = stringToChar(variable, config.getString(variable));
                    break;
                default:
                    value = "";
                    break;
            }
        } else {
            switch (type) {
                case STRING:
                    break;
                case INT:
                    value = DEFAULT_INT;
                    break;
                case LONG:
                    value = DEFAULT_LONG;
                    break;
                case BOOLEAN:
                    value = DEFAULT_BOOLEAN;
                    break;
                case CHAR:
                    value = DEFAULT_CHAR;
                    break;
            }
        }
        allProperties.put(variable, value);
        return (T) value;
    }

    private static Character stringToChar(String propertyName, String propertyValue) {
        if (propertyValue.length() != 1) {
            throw new IllegalArgumentException(
                    format("Single character is expected in '%s' variable, but got '%s'", propertyName, propertyValue));
        }
        return propertyValue.charAt(0);
    }

    public <T> T get(String variable) {
        return get(variable, ConfigTypes.STRING);
    }

    public <T> T get(String variable, T defaultValue) {
        return get(variable, ConfigTypes.STRING, defaultValue);
    }

    public Map<String, Object> getAllProperties() {
        return allProperties;
    }
}