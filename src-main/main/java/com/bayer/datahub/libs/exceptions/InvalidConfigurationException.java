package com.bayer.datahub.libs.exceptions;

import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

public class InvalidConfigurationException extends RuntimeException {
    private static final String CHECK_MESSAGE = "Please check your configuration.";

    public InvalidConfigurationException(String setting, Object currentValue) {
        super(formatPropertyName(setting, currentValue) + " " + CHECK_MESSAGE);
    }

    public InvalidConfigurationException(String setting, Object currentValue, Object[] possibleValues) {
        this(setting, currentValue, possibleValues, null);
    }

    public InvalidConfigurationException(String setting, Object currentValue, Object[] possibleValues, Throwable t) {
        super(formatPropertyName(setting, currentValue) + " " + formatPossibleValue(possibleValues) + " " + CHECK_MESSAGE, t);
    }

    private static String formatPropertyName(String setting, Object currentValue) {
        return format("Could not find a valid setting for '%s'. Current value: '%s'.", setting, currentValue);
    }

    private static String formatPossibleValue(Object[] possibleValues) {
        if (possibleValues == null || possibleValues.length == 0) {
            return "Possible values: No possible values.";
        }
        var values = Stream.of(possibleValues).map(s -> "'" + s + "'").collect(Collectors.joining(", "));
        return "Possible values: " + values + ".";
    }
}
