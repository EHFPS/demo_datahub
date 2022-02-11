package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class OracleDbContextTest {

    @Test
    void getMinValueInColumn() {
        var context = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
                DB_HOST, "the_host",
                DB_PORT, "1234",
                DB_SERVICE_NAME, "the_service"))
                .build().getInstance(OracleDbContext.class);
        var str = context.getConnectionString();
        var expStr = "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)" +
                "(HOST=the_host)(PORT=1234)))(CONNECT_DATA=(SERVICE_NAME=the_service)))";
        assertThat(str, equalTo(expStr));
    }
}