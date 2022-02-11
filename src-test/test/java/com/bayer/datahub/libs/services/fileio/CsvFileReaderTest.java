package com.bayer.datahub.libs.services.fileio;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static com.bayer.datahub.ResourceHelper.resourceToPath;
import static com.bayer.datahub.libs.services.fileio.CsvRecordDelimiter.LF;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CsvFileReaderTest {
    private static final char CSV_VALUE_DELIMITER = ';';
    private static final Path PATH = resourceToPath(CsvFileReaderTest.class, "CsvFileReaderTest.csv");

    @Test
    void read() throws IOException {
        FileReader fileReader = new CsvFileReader(CSV_VALUE_DELIMITER, LF.name(), UTF_8.name(), 500, "");
        fileReader.open(PATH);
        List<SimpleRecord> read = fileReader.read();
        fileReader.close();
        assertThat(read, hasSize(2));
        assertThat(read.get(0).toString(), equalTo(
                "\n    Column: fieldA Value: valueA1 Data type: string" +
                        "\n    Column: fieldB Value: 123 Data type: string"));
        assertThat(read.get(1).toString(), equalTo(
                "\n    Column: fieldA Value: valueA2 Data type: string" +
                        "\n    Column: fieldB Value: 456 Data type: string"));
    }

    /**
     * Bug https://bijira.intranet.cnb/browse/DAAAA-2507
     */
    @Test
    void readTabValueDelimiter() throws IOException {
        Path path = resourceToPath(CsvFileReaderTest.class, "CsvFileReaderTest_readTabValueDelimiter.csv");
        FileReader fileReader = new CsvFileReader('\t', LF.name(), UTF_8.name(), 500, "");
        fileReader.open(path);
        List<SimpleRecord> read = fileReader.read();
        fileReader.close();
        assertThat(read, hasSize(2));
        assertThat(read.get(0).toString(), equalTo(
                "\n    Column: fieldA Value: valueA1 Data type: string" +
                        "\n    Column: fieldB Value: 123 Data type: string"));
        assertThat(read.get(1).toString(), equalTo(
                "\n    Column: fieldA Value: valueA2 Data type: string" +
                        "\n    Column: fieldB Value: 456 Data type: string"));
    }

    @Test
    void hasNextLine() throws IOException {
        FileReader fileReader = new CsvFileReader(CSV_VALUE_DELIMITER, LF.name(), UTF_8.name(), 500, "");
        fileReader.open(PATH);
        assertTrue(fileReader.hasNextLine());
        fileReader.read();

        assertFalse(fileReader.hasNextLine());
        fileReader.close();
    }
}