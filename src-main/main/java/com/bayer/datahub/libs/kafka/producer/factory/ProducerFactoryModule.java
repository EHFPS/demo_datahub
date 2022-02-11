package com.bayer.datahub.libs.kafka.producer.factory;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;

public class ProducerFactoryModule extends AbstractModule {
    public static final String MAIN_PRODUCER_FACTORY = "main-producer-factory";

    @Override
    protected void configure() {
        bind(ProducerFactory.class).annotatedWith(Names.named(MAIN_PRODUCER_FACTORY)).to(ProducerFactoryImpl.class);
    }
}
