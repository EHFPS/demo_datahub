package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.config.ConfigFileLoader;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.config.ConfigsList;
import com.bayer.datahub.libs.kafka.ClientModule;
import com.bayer.datahub.libs.services.common.CommonServicesModule;
import com.google.inject.AbstractModule;

public class AppModule extends AbstractModule {
    private final Configs configs;

    public AppModule(Configs configs) {
        this.configs = configs;
    }

    @Override
    protected void configure() {
        bind(Configs.class).toInstance(configs);
        install(new CommonServicesModule());
        install(new ClientModule(configs));
    }

    protected ConfigsList loadConfigsList() {
        return ConfigFileLoader.readConfigsListFromSystemProperty();
    }
}
