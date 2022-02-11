package com.bayer.datahub.libs.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static com.bayer.datahub.ReflectionHelper.readStaticField;
import static com.bayer.datahub.libs.config.ConfigParser.ConfigTypes.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ConfigParserTest {
    private static final String PROPERTY = "the.property";

    @Test
    void returnValueIfPropertyIsPresent() {
        assertIfPresent(STRING, "abc", "def");
        assertIfPresent(STRING, "\t", "def");
        assertIfPresent(STRING, " ", "def");
        assertIfPresent(STRING, "  ", "def");
        assertIfPresent(INT, 9, 7);
        assertIfPresent(LONG, 5L, 3L);
        assertIfPresent(BOOLEAN, true, false);
        assertIfPresent(BOOLEAN, false, true);
        assertIfPresent(CHAR, 'K', 'B');
        assertIfPresent(CHAR, '\t', 'B');
        assertIfPresent(CHAR, ' ', 'B');
    }

    @Test
    void returnDefaultValueIfPropertyAbsents() {
        assertDefaultIfAbsent(STRING, "def");
        assertDefaultIfAbsent(INT, 7);
        assertDefaultIfAbsent(LONG, 5L);
        assertDefaultIfAbsent(BOOLEAN, true);
        assertDefaultIfAbsent(BOOLEAN, false);
        assertDefaultIfAbsent(CHAR, 'K');
    }

    @Test
    void returnDefaultValueIfPropertyIsEmpty() {
        assertDefaultIfEmpty(STRING, "def");
        assertDefaultIfEmpty(INT, 7);
        assertDefaultIfEmpty(LONG, 5L);
        assertDefaultIfEmpty(BOOLEAN, true);
        assertDefaultIfEmpty(BOOLEAN, false);
        assertDefaultIfEmpty(CHAR, 'K');
    }

    @Test
    void returnAppDefaultValueIfPropertyIsEmpty() {
        assertAppDefaultIfEmpty(STRING, "");
        assertAppDefaultIfEmpty(INT, readStaticField(ConfigParser.class, "DEFAULT_INT"));
        assertAppDefaultIfEmpty(LONG, readStaticField(ConfigParser.class, "DEFAULT_LONG"));
        assertAppDefaultIfEmpty(BOOLEAN, readStaticField(ConfigParser.class, "DEFAULT_BOOLEAN"));
        assertAppDefaultIfEmpty(CHAR, readStaticField(ConfigParser.class, "DEFAULT_CHAR"));
    }

    private static <T> void assertIfPresent(ConfigParser.ConfigTypes type, T expValue, T defaultValue) {
        Properties properties = new Properties();
        properties.put(PROPERTY, expValue.toString());
        Config conf = ConfigFactory.parseProperties(properties);
        ConfigParser parser = new ConfigParser(conf);
        assertThat(parser.get(PROPERTY, type, defaultValue), equalTo(expValue));
    }

    private static <T> void assertDefaultIfAbsent(ConfigParser.ConfigTypes type, T defaultValue) {
        Config conf = ConfigFactory.empty();
        ConfigParser parser = new ConfigParser(conf);
        assertThat(parser.get(PROPERTY, type, defaultValue), equalTo(defaultValue));
    }

    private static <T> void assertDefaultIfEmpty(ConfigParser.ConfigTypes type, T defaultValue) {
        Properties properties = new Properties();
        String propertyEmpty = "the.property.1";
        properties.put(propertyEmpty, "");
        Config conf = ConfigFactory.parseProperties(properties);
        ConfigParser parser = new ConfigParser(conf);
        assertThat(parser.get(propertyEmpty, type, defaultValue), equalTo(defaultValue));
    }

    private static <T> void assertAppDefaultIfEmpty(ConfigParser.ConfigTypes type, T appDefaultValue) {
        Properties properties = new Properties();
        String propertyEmpty = "the.property.1";
        properties.put(propertyEmpty, "");
        Config conf = ConfigFactory.parseProperties(properties);
        ConfigParser parser = new ConfigParser(conf);
        assertThat(parser.get(propertyEmpty, type), equalTo(appDefaultValue));
    }
}