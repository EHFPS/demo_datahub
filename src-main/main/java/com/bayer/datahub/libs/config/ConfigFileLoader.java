package com.bayer.datahub.libs.config;

import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigFileLoader {
    private static final Logger log = LoggerFactory.getLogger(ConfigFileLoader.class);
    public static final String CONFIG_FILE_PROPERTY_NAME = "config.file";

    public static ConfigsList readConfigsListFromSystemProperty() {
        return readConfigsListFromFile(getConfigFileFromSystemProperty());
    }

    public static ConfigsList readConfigsListFromFile(File configFile) {
        ConfigsList result;
        if (isPropertiesFile(configFile) || isConfFile(configFile)) {
            result = propertiesFileToConfigsList(configFile);
        } else if (isYamlFile(configFile)) {
            result = yamlFileToConfigsList(configFile);
        } else {
            throw new IllegalArgumentException("Config file format is not supported (use properties or YAML): " +
                    configFile.getAbsolutePath());
        }
        return result;
    }

    private static ConfigsList yamlFileToConfigsList(File yamlFile) {
        var json = yamlToJson(yamlFile);
        var configOrigin = ConfigFactory.parseString(json).resolve();
        var clientType = ClientType.parse(configOrigin.getString("client.type"));
        final var clientDefaultKey = "client.default";
        var clientDefaultConfig = configOrigin.hasPath(clientDefaultKey)
                ? configOrigin.getConfig(clientDefaultKey) : ConfigFactory.empty();
        final var dbKey = "db";
        var defaultDbConfig = clientDefaultConfig.hasPath(dbKey) ?
                clientDefaultConfig.getConfig(dbKey).atPath(dbKey) : ConfigFactory.empty();
        var configList = configOrigin.getConfigList("client.configs");
        List<Config> result = new ArrayList<>();
        for (Config clientConfig : configList) {
            var clientKeyPrefix = clientType.name().toLowerCase().replace("_", ".");
            var clientConfigAtPath = clientConfig.withFallback(clientDefaultConfig).atPath(clientKeyPrefix);
            var dbConf = clientConfig.hasPath(dbKey) ?
                    clientConfig.getConfig(dbKey).atPath(dbKey).withFallback(defaultDbConfig) : defaultDbConfig;
            var tableKay = "table";
            var dbTableKey = dbKey + ".table";
            dbConf = clientConfig.hasPath(tableKay) ? dbConf.withValue(dbTableKey, clientConfig.getValue(tableKay)) : dbConf;
            var topic = clientConfig.getValue("topic");
            var conf = splitKeysByDots(configOrigin.withFallback(clientConfigAtPath).withFallback(dbConf)
                    .withValue(PropertyNames.KAFKA_COMMON_TOPIC_PROPERTY, topic));
            result.add(conf);
        }
        return convertConfigListToConfigList(result);
    }

    private static ConfigsList propertiesFileToConfigsList(File propertiesFile) {
        return convertConfigListToConfigList(List.of(ConfigFactory.parseFile(propertiesFile).resolve()));
    }

    private static File getConfigFileFromSystemProperty() {
        var configFilePropertyValue = System.getProperty(CONFIG_FILE_PROPERTY_NAME);
        if (configFilePropertyValue == null) {
            throw new IllegalArgumentException("System property " + CONFIG_FILE_PROPERTY_NAME + " was not found.");
        }
        return new File(configFilePropertyValue);
    }

    private static ConfigsList convertConfigListToConfigList(List<Config> configList) {
        var configsList = new ConfigsList(configList.stream().map(Configs::new).collect(Collectors.toList()));
        log.info(configsList.toString());
        return configsList;
    }

    /**
     * Convert "a.b.c: v" YAML properties to
     * <pre>
     * "a:
     *    b:
     *      c: v"
     * </pre>
     */
    private static Config splitKeysByDots(Config conf) {
        var map = new HashMap<String, Object>();
        for (var entry : conf.entrySet()) {
            var key = entry.getKey().replace("\"", "");
            map.put(key, entry.getValue());
        }
        return ConfigFactory.parseMap(map);
    }

    private static String yamlToJson(File yamlFile) {
        try {
            var yamlMapper = new ObjectMapper(new YAMLFactory());
            var object = yamlMapper.readValue(yamlFile, Object.class);
            var jsonMapper = new ObjectMapper();
            return jsonMapper.writeValueAsString(object);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean isPropertiesFile(File configFile) {
        return configFile.getName().toLowerCase().endsWith(".properties");
    }

    private static boolean isConfFile(File configFile) {
        return configFile.getName().toLowerCase().endsWith(".conf");
    }

    private static boolean isYamlFile(File configFile) {
        return configFile != null && (configFile.getName().toLowerCase().endsWith(".yaml") ||
                configFile.getName().toLowerCase().endsWith(".yml"));
    }
}
