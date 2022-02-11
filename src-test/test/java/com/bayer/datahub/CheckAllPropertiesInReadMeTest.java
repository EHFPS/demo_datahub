package com.bayer.datahub;

import com.bayer.datahub.libs.config.PropertyNames;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Check that all properties from {@link PropertyNames} class are declared in README.md
 */
class CheckAllPropertiesInReadMeTest {
    private static final File readMeFile = readReadmeFile();
    private static final String readMeContent = readReadMeContent();
    private static final List<String> fields = getFieldNames();

    private static File readReadmeFile() {
        var rootProjectDir = System.getenv("project.dir");
        var readMeFile = new File(rootProjectDir, "README.md");
        if (!readMeFile.exists()) {
            throw new RuntimeException("README file not found: " + readMeFile.getAbsolutePath());
        }
        return readMeFile;
    }

    private static List<String> findMissingProperties() {
        String[] exclusions = {"consumer.s3.aws.service.endpoint"};
        return fields.stream()
                .filter(field -> !readMeContent.contains(field))
                .filter(field -> !StringUtils.startsWithAny(field, exclusions))
                .collect(Collectors.toList());
    }

    private static List<String> findExcessProperties(List<String> existingProperties) {
        String[] exclusions = {"kafka.common.", "kafka.consumer.", "kafka.producer."};
        return existingProperties.stream()
                .filter(property -> !fields.contains(property))
                .filter(field -> !StringUtils.startsWithAny(field, exclusions))
                .collect(Collectors.toList());
    }

    private static void assertMissingProperties(List<String> missedPropertyNames) {
        var className = PropertyNames.class.getName();
        if (!missedPropertyNames.isEmpty()) {
            var message = String.format(
                    "These %d properties (of %d) from '%s' are NOT mentioned in '%s' (please, update README.md):\n%s\n",
                    missedPropertyNames.size(), fields.size(), className, readMeFile.getAbsolutePath(),
                    String.join("\n", missedPropertyNames));
            throw new AssertionError(message);
        }
    }

    private static void assertExcessProperties(List<String> excessPropertyNames) {
        var className = PropertyNames.class.getName();
        if (!excessPropertyNames.isEmpty()) {
            var message = String.format(
                    "These %d properties (of %d) are mentioned in '%s', but absent in '%s' (please, update README.md):\n%s\n",
                    excessPropertyNames.size(), fields.size(), readMeFile.getAbsolutePath(), className,
                    String.join("\n", excessPropertyNames));
            throw new AssertionError(message);
        }
    }

    private static List<String> findExistingProperties() {
        var p = Pattern.compile("\\|\\s*`([.\\w]+)`\\s*\\|");
        var m = p.matcher(readMeContent);
        var existingProperties = new ArrayList<String>();
        while (m.find()) {
            var property = m.group(1);
            existingProperties.add(property);
        }
        return existingProperties;
    }

    private static List<String> getFieldNames() {
        try {
            var excessPropertyNames = new ArrayList<String>();
            for (var field : PropertyNames.class.getFields()) {
                excessPropertyNames.add((String) field.get(null));
            }
            return excessPropertyNames;
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readReadMeContent() {
        try {
            return FileUtils.readFileToString(readMeFile);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void checkMissedProperties() {
        var missedProperties = findMissingProperties();
        assertMissingProperties(missedProperties);
    }

    @Test
    void checkExcessProperties() {
        var existingProperties = findExistingProperties();
        var excessProperties = findExcessProperties(existingProperties);
        assertExcessProperties(excessProperties);
    }
}
