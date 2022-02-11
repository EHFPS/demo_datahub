package com.bayer.datahub.libs.services.fileio;

import org.junit.jupiter.api.Test;

import static com.bayer.datahub.libs.services.fileio.TypeConverter.convertStringToType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TypeConverterTest {

    @Test
    void testConvertStringToType() {
        assertThat(convertStringToType("CHAR", "A"), equalTo("A"));
    }

    /**
     * Bug "DAAAA-1908 TypeConverter treats SQL DATE as TIMESTAMP".
     * Oracle Database returns DATE type as TIMESTAMP (e.g. "2020-11-30 00:00:00.0" instead of "2020-11-30").
     * So DATE should be converted appropriately.
     */
    @Test
    void covertOracleDate() {
        assertThat(convertStringToType("DATE", "2020-11-30 00:00:00.0"), equalTo("2020-11-30"));
        assertThat(convertStringToType("DATE", "2020-11-30"), equalTo("2020-11-30"));
        assertThrows(IllegalArgumentException.class, () -> convertStringToType("DATE", "not a date"));
    }
}