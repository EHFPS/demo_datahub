package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.config.ConfigsList;

import java.util.List;

public class TestAppModule extends AppModule {
    private final Configs configs;

    public TestAppModule(Configs configs) {
        super(configs);
        this.configs = configs;
    }

    @Override
    protected ConfigsList loadConfigsList() {
        return new ConfigsList(List.of(configs));
    }
}
