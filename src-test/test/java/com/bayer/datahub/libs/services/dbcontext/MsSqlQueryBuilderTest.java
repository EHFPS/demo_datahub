package com.bayer.datahub.libs.services.dbcontext;

import com.bayer.datahub.FactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class MsSqlQueryBuilderTest {
    private final MsSqlQueryBuilder builder = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name(),
            DB_NAME, "the_database",
            DB_SCHEMA, "the_schema",
            DB_TABLE, "the_table"))
            .build().getInstance(MsSqlQueryBuilder.class);

    @Test
    void getInsertQueryStart() {
        var query = builder.getInsertQueryStart();
        assertThat(query, equalTo("INSERT INTO the_database.the_schema.the_table VALUES ("));
    }

    @Test
    void getUpdateQueryStart() {
        var query = builder.getUpdateQueryStart();
        assertThat(query, equalTo("UPDATE the_database.the_schema.the_table SET "));
    }

    @Test
    void buildInsertQuery() {
        var fields = Arrays.asList("fieldA", "fieldB");
        var query = builder.buildInsertQuery(fields);
        assertThat(query, equalTo("INSERT INTO the_database.the_schema.the_table(\"fieldA\", \"fieldB\") " +
                "VALUES (?, ?)"));
    }

    @Test
    void buildUpsertQuery() {
        var fields = Arrays.asList("fieldA", "fieldB");
        var key = "fieldA";
        var query = builder.buildUpsertQuery(fields, key);
        assertThat(query, equalTo("BEGIN TRY INSERT INTO the_database.the_schema.the_table(\"fieldA\", \"fieldB\") " +
                "VALUES (?, ?) END TRY " +
                "BEGIN CATCH IF ERROR_NUMBER() IN (2601, 2627) " +
                "UPDATE the_database.the_schema.the_table SET fieldB = ? WHERE fieldA = ? END CATCH"));
    }
}