package com.bayer.datahub;

import com.bayer.datahub.kafkaclientbuilder.TestAppModule;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.SchemaRegistryError;
import com.bayer.datahub.libs.interfaces.DbContext;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactory;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactory;
import com.bayer.datahub.libs.services.common.date.DateService;
import com.bayer.datahub.libs.services.common.date.TestDateService;
import com.bayer.datahub.libs.services.dbcontext.H2DbContext;
import com.bayer.datahub.libs.services.dbcontext.H2QueryBuilder;
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryService;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.name.Names;
import com.google.inject.util.Modules;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.Pair;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule.MAIN_PRODUCER_FACTORY;
import static com.bayer.datahub.libs.services.common.notification.kafka.KafkaNotificationModule.KAFKA_NOTIFICATION_PRODUCER_FACTORY;
import static org.mockito.Mockito.*;

public class FactoryBuilder extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());
    private final Configs configs;
    private final Map<Class, Object> bindOverrides = new HashMap<>();
    private final Map<Pair<Class, String>, Object> namedBindOverrides = new HashMap<>();
    private final DateService dateService = new TestDateService();

    private FactoryBuilder(Config config) {
        configs = new Configs(config);
    }

    public static FactoryBuilder newBuilder(Map<String, String> configMap) {
        ConfigFactory.invalidateCaches();
        return new FactoryBuilder(ConfigFactory.parseMap(configMap));
    }

    private static Object spyObject(Object instance) {
        var mockingDetails = Mockito.mockingDetails(instance);
        return mockingDetails.isMock() || mockingDetails.isSpy() ? instance : spy(instance);
    }

    public FactoryBuilder override(Class clazz, Object object) {
        bindOverrides.put(clazz, object);
        return this;
    }

    public FactoryBuilder override(Class clazz, String name, Object object) {
        namedBindOverrides.put(Pair.of(clazz, name), object);
        return this;
    }

    public FactoryBuilder override(String clazz, Object object) {
        try {
            bindOverrides.put(Class.forName(clazz), object);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public FactoryBuilder withStringProducerFactory() {
        return override(ProducerFactory.class, MAIN_PRODUCER_FACTORY, KAFKA_CLIENT_HELPER.createStringProducerFactory());
    }

    public FactoryBuilder withKafkaNotificationProducerFactory() {
        return override(ProducerFactory.class, KAFKA_NOTIFICATION_PRODUCER_FACTORY, KAFKA_CLIENT_HELPER.createStringProducerFactory());
    }

    public FactoryBuilder withByteArrayProducerFactory() {
        return override(ProducerFactory.class, MAIN_PRODUCER_FACTORY, KAFKA_CLIENT_HELPER.createByteProducerFactory());
    }

    public FactoryBuilder withAvroProducerFactory() {
        return override(ProducerFactory.class, MAIN_PRODUCER_FACTORY, KAFKA_CLIENT_HELPER.createAvroProducerFactory());
    }

    public FactoryBuilder withStringConsumerFactory() {
        return override(ConsumerFactory.class, KAFKA_CLIENT_HELPER.createStringConsumerFactory());
    }

    public FactoryBuilder withH2DbContext(String h2DbFile) {
        var queryBuilder = new H2QueryBuilder(configs);
        return override(DbContext.class, new H2DbContext(queryBuilder, configs, h2DbFile));
    }

    public FactoryBuilder withSchemaRegistryMock() {
        return withSchemaRegistryMock(null);
    }

    public FactoryBuilder withSchemaRegistryMock(Schema schema) {
        try {
            var schemaRegistryService = mock(SchemaRegistryService.class);
            when(schemaRegistryService.getSchema()).thenReturn(schema);
            return override(SchemaRegistryService.class, schemaRegistryService);
        } catch (SchemaRegistryError e) {
            throw new RuntimeException(e);
        }
    }

    public FactoryBuilder withByteArrayConsumerFactory() {
        return override(ConsumerFactory.class, KAFKA_CLIENT_HELPER.createBinaryConsumerFactory(new Properties()));
    }

    public FactoryBuilder withByteArrayConsumerFactory(Properties overrideConsumerConfig) {
        return override(ConsumerFactory.class, KAFKA_CLIENT_HELPER.createBinaryConsumerFactory(overrideConsumerConfig));
    }

    public FactoryBuilder withStringConsumerFactory(Properties overrideConsumerConfig) {
        return override(ConsumerFactory.class, KAFKA_CLIENT_HELPER.createStringConsumerFactory(overrideConsumerConfig));
    }

    public FactoryBuilder withAvroConsumerFactory() {
        return override(ConsumerFactory.class, KAFKA_CLIENT_HELPER.createAvroConsumerFactory(new Properties()));
    }

    public FactoryBuilder withAvroConsumerFactory(Properties overrideConsumerConfig) {
        return override(ConsumerFactory.class, KAFKA_CLIENT_HELPER.createAvroConsumerFactory(overrideConsumerConfig));
    }

    public FactoryBuilder withTestDateService() {
        return override(DateService.class, dateService);
    }

    public Factory build() {
        var injector = Guice.createInjector(Modules.override(new TestAppModule(configs))
                .with(new AbstractModule() {
                    @Override
                    protected void configure() {
                        bindOverrides.forEach((clazz, instance) -> bind(clazz).toInstance(spyObject(instance)));
                        namedBindOverrides.forEach((pair, instance) -> bind(pair.getKey())
                                .annotatedWith(Names.named(pair.getValue()))
                                .toInstance(spyObject(instance)));
                    }
                })
        );
        return new Factory(injector);
    }

    public static class Factory {
        private final Injector injector;

        private Factory(Injector injector) {
            this.injector = injector;
        }

        public <T> T getInstance(Class<T> clazz) {
            return injector.getInstance(clazz);
        }

        public <T> T getInstance(Class<T> clazz, String name) {
            return injector.getInstance(Key.get(clazz, Names.named(name)));
        }

        public KafkaClient getKafkaClient() {
            return getInstance(KafkaClient.class);
        }
    }
}
