package com.bayer.datahub;

import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ResourceHelper {

    public static byte[] resourceToBytes(Class<?> clazz, String resource) {
        try {
            URL url = clazz.getResource(resource);
            if (url == null) {
                throw new RuntimeException("Resource is not found: " + resource);
            }
            File file = new File(url.toURI());
            return Files.asByteSource(file).read();
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String resourceToString(Class<?> clazz, String resource) {
        try {
            URL url = clazz.getResource(resource);
            if (url == null) {
                throw new RuntimeException("Resource is not found: " + resource);
            }
            File file = new File(url.toURI());
            return Files.asCharSource(file, StandardCharsets.UTF_8).read();
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path resourceToPath(Class<?> clazz, String resource) {
        try {
            return Paths.get(clazz.getResource(resource).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static File resourceToFile(Class<?> clazz, String resource) {
        return resourceToPath(clazz, resource).toFile();
    }

    public static String resourceToAbsolutePath(Class<?> clazz, String resource) {
        try {
            return new File((clazz.getResource(resource).toURI())).getAbsolutePath();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
