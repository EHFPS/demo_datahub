package com.bayer.datahub.libs.kafka.consumer.factory;

import com.google.inject.AbstractModule;

public class ConsumerFactoryModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(ConsumerFactory.class).to(ConsumerFactoryImpl.class);
    }
}
