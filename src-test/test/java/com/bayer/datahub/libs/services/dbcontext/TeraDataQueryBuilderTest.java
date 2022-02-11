package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class TeraDataQueryBuilderTest {
    private final TeraDataQueryBuilder builder = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_NAME, "the_database",
            DB_TABLE, "the_table"))
            .build().getInstance(TeraDataQueryBuilder.class);

    @Test
    void getInsertQueryStart() {
        var start = builder.getInsertQueryStart();
        assertThat(start, equalTo("INSERT INTO the_database.the_table VALUES ("));
    }

    @Test
    void buildSelectAllQuery() {
        var start = builder.buildSelectAllQuery();
        assertThat(start, equalTo("select * from the_database.the_table"));
    }

    @Test
    void getUpdateQueryStart() {
        var start = builder.getUpdateQueryStart();
        assertThat(start, equalTo("update the_database.the_table set "));
    }

    @Test
    void columnQuery() {
        var columnQuery = builder.columnQuery();
        assertThat(columnQuery, equalTo("select top 1 * from the_database.the_table"));
    }

    @Test
    void buildUpsertQuery() {
        var fields = Arrays.asList("fieldA", "fieldB");
        var key = "the_key";
        var actQuery = builder.buildUpsertQuery(fields, key);
        var expQuery = "update the_database.the_table set fieldA = ?fieldB = ?,  where the_key = ? " +
                "else insert into the_database.the_table(fieldA, fieldB) values (?, ?)";
        assertThat(actQuery, equalTo(expQuery));
    }
}