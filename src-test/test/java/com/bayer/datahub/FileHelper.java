package com.bayer.datahub;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.io.Files.asCharSource;

public class FileHelper {
    public static String readFileContent(Path path) {
        try {
            return asCharSource(path.toFile(), Charset.defaultCharset()).read();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Creates a path to a temp file, but doesn't create the file.
     */
    public static Path createPathToTempFile(String fileName, String suffix) {
        try {
            Path path = Files.createTempFile(fileName, suffix);
            Files.delete(path);
            return path;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
