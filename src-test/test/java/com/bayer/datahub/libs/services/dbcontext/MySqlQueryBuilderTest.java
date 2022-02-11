package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.DB_TABLE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class MySqlQueryBuilderTest {
    private final MySqlQueryBuilder builder = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_TABLE, "the_table"))
            .build().getInstance(MySqlQueryBuilder.class);

    @Test
    void getInsertQueryStart() {
        var start = builder.getInsertQueryStart();
        assertThat(start, equalTo("INSERT INTO the_table VALUES ("));
    }

    @Test
    void columnQuery() {
        var columnQuery = builder.columnQuery();
        assertThat(columnQuery, equalTo("select * from the_table limit 1"));
    }

    @Test
    void buildUpsertQuery() {
        var fields = Arrays.asList("fieldA", "fieldB");
        var key = "the_key";
        var actQuery = builder.buildUpsertQuery(fields, key);
        var expQuery = "insert into the_table(fieldA, fieldB) values (?, ?) " +
                "on duplicate key update fieldA = ?, fieldB = ?";
        assertThat(actQuery, equalTo(expQuery));
    }
}