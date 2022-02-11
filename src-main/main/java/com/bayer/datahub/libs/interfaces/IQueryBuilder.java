package com.bayer.datahub.libs.interfaces;


import com.bayer.datahub.libs.services.dbcontext.SchemaTable;
import org.apache.avro.generic.GenericRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * For creating queries from avro records, {@literal i.e.} from a Kafka topic
 */
public interface IQueryBuilder {
    /**
     * Creates a query to be used with a preparedStatement object, generally in the form of
     * INSERT INTO 'TABLE' VALUES (?, ?, ...., ?)
     */
    String buildInsertQuery(List<String> fields);

    /**
     * @return select * from target table
     */
    String buildSelectAllQuery();

    /**
     * select arg1, arg2,..., argN from target table
     */
    String buildSelectAllQuery(String... args);

    String buildUpdateQuery(List<String> fields, String primaryKey);

    String buildDeltaQuery(String deltaColumn, boolean withMin, boolean withMax, boolean minIncluded);

    String buildUpsertQuery(List<String> cols, String key);

    void setString(PreparedStatement ps, int pos, String value) throws SQLException;

    String columnQuery();

    /**
     * Returns a count of the number of rows in the table
     */
    String buildRowCountQuery();

    /**
     * Populates values in a jdbc PreparedStatement object from a GenericRecord {@literal i.e.} from
     * a Kafka topic
     *
     * @param ps - the preparedStatement object
     * @return - PreparedStatement with parameters
     */
    void populateQuery(PreparedStatement ps, GenericRecord record, SchemaTable schemaTable);

    void populateQuery(PreparedStatement ps, GenericRecord record, SchemaTable schemaTable,
                                    String primaryKeyValue) throws SQLException;

    String buildRangeQueryGreaterThan(String inputQuery, String col, String min);

    String buildRangeQueryLessThan(String inputQuery, String col, String max);

    String buildRangeQuery(String inputQuery, String col, String min, String max);

    String minValueInColumnQuery(String column);

    String maxValueInColumnQuery(String column);
}
