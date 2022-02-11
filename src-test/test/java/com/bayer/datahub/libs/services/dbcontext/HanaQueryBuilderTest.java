package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class HanaQueryBuilderTest {
    private final HanaQueryBuilder builder = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_NAME, "the_database",
            DB_TABLE, "the_table"))
            .build().getInstance(HanaQueryBuilder.class);

    @Test
    void getInsertQueryStart() {
        var query = builder.getInsertQueryStart();
        assertThat(query, equalTo("INSERT INTO \"the_database\".\"the_table\" VALUES ("));
    }

    @Test
    void getUpdateQueryStart() {
        var query = builder.getUpdateQueryStart();
        assertThat(query, equalTo("update \"the_database\".\"the_table\" set "));
    }

    @Test
    void buildSelectAllQuery() {
        var query = builder.buildSelectAllQuery();
        assertThat(query, equalTo("select * from \"the_database\".\"the_table\""));
    }

    @Test
    void buildUpsertQuery() {
        var fields = Arrays.asList("fieldA", "fieldB");
        var key = "fieldA";
        var query = builder.buildUpsertQuery(fields, key);
        assertThat(query, equalTo("upsert \"the_database\".\"the_table\" (fieldA, fieldB) values (?, ?) " +
                "where fieldA = ?"));
    }
}