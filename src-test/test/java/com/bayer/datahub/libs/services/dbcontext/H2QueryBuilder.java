package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.libs.config.Configs;

import javax.inject.Inject;

public class H2QueryBuilder extends BaseQueryBuilder {
    @Inject
    public H2QueryBuilder(Configs configs) {
        super(configs);
    }

    @Override
    protected String getInsertQueryStart() {
        throw new UnsupportedOperationException();
    }
}
