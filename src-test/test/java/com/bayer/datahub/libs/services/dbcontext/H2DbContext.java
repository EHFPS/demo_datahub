package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.IQueryBuilder;

import javax.inject.Inject;

import static java.lang.String.format;

/**
 * Implementation for test purposes.
 */
public class H2DbContext extends BaseDbContext {
    private final String h2DbFile;

    @Inject
    public H2DbContext(IQueryBuilder builder, Configs configs, String h2DbFile) {
        super(builder, configs);
        this.h2DbFile = h2DbFile;
    }

    @Override
    public String getConnectionString() {
        return format("jdbc:h2:%s", h2DbFile);
    }

    @Override
    protected String getDriverClass() {
        return "org.h2.Driver";
    }
}
