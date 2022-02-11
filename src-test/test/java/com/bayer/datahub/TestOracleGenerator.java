package com.bayer.datahub;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;

/**
 * Fill an Oracle table with huge amount of data for testing purposes.
 * Destination table:
 * CREATE TABLE huge_table_iablokov (
 * id VARCHAR(40),
 * data VARCHAR(100),
 * created TIMESTAMP
 * );
 */
public class TestOracleGenerator {
    private static final Logger log = LoggerFactory.getLogger(TestOracleGenerator.class);

    public static void main(String[] args) throws SQLException {
        var rowNumber = Integer.parseInt(System.getProperty("rowNumber", "3"));
        var login = Objects.requireNonNull(System.getProperty("login"));
        var password = Objects.requireNonNull(System.getProperty("password"));
        var url = "jdbc:oracle:thin:@by-ebvs17d.de.bayer.cnb:1522:EBVS17D";
        var random = new Random();
        var created = LocalDateTime.now().minusSeconds(rowNumber);
        var formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
        log.info("Connecting to Oracle...");
        var updatedRows = 0;
        try (var connection = DriverManager.getConnection(url, login, password);
             var statement = connection.prepareStatement("INSERT INTO huge_table_iablokov(id, data, created) " +
                     "VALUES (?, ?, TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS'))")) {
            log.info("Connected to Oracle.");
            log.info("Generating rows...");
            for (int i = 0; i < rowNumber; i++) {
                var id = UUID.randomUUID().toString();
                var data = "data-" + random.nextInt(Integer.MAX_VALUE);
                created = created.plusSeconds(1);
                var createdStr = formatter.format(created);
                statement.setString(1, id);
                statement.setString(2, data);
                statement.setString(3, createdStr);
                statement.addBatch();
            }
            var updateCounters = statement.executeBatch();
            updatedRows = Arrays.stream(updateCounters).sum();
        }
        log.info("Generated rows: {}", updatedRows);
    }
}
