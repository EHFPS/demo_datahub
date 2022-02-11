package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.config.ConfigFileLoader;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.google.inject.Guice;
import org.junit.jupiter.api.Test;

import static com.bayer.datahub.ResourceHelper.resourceToFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

class AppModuleTest {

    @Test
    void configureDeltaDbProducer() {
        var configResource = "AppModuleTest_configureDeltaDbProducer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.producer.db.delta.DeltaDbProducer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configurePlainDbProducer() {
        var configResource = "AppModuleTest_configurePlainDbProducer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.producer.db.plain.PlainDbProducer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configureCsvProducer() {
        var configResource = "AppModuleTest_configureCsvProducer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.producer.csv.CsvProducer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configureShareDocProducer() {
        var configResource = "AppModuleTest_configureShareDocProducer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocProducer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configureS3Consumer() {
        var configResource = "AppModuleTest_configureS3Consumer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.consumer.cloud.CloudConsumer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configureFileConsumer() {
        var configResource = "AppModuleTest_configureFileConsumer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.consumer.file.FileConsumer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configureDbConsumer() {
        var configResource = "AppModuleTest_configureDbConsumer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.consumer.db.DbConsumer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    @Test
    void configureSplunkConsumer() {
        var configResource = "AppModuleTest_configureSplunkConsumer.properties";
        var expKafkaClientClass = "com.bayer.datahub.libs.kafka.consumer.splunk.SplunkConsumer";
        configureAppAndAssert(configResource, expKafkaClientClass);
    }

    private static void configureAppAndAssert(String configResource, String expKafkaClientClass) {
        var configFile = resourceToFile(AppModuleTest.class, configResource);
        var configs = ConfigFileLoader.readConfigsListFromFile(configFile).get(0);
        var injector = Guice.createInjector(new AppModule(configs));
        var clientManager = injector.getInstance(ClientManager.class);
        assertThat(clientManager, notNullValue());

        var client = injector.getInstance(KafkaClient.class);
        assertThat(client.getClass().getName(), equalTo(expKafkaClientClass));
    }
}