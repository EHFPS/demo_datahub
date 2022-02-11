package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_SHAREDOC;
import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_REPOSITORY_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.OBJECT_ID_KEY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.STUDY_NAME_KEY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FilenameGeneratorTest {
    private final FilenameGenerator generator = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name(),
            PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, "the_repo"))
            .build()
            .getInstance(FilenameGenerator.class);

    @Test
    void getMetadataFilePath() {
        Map<String, Object> rowProperties = new HashMap<>();
        var expObjectId = "090192f28025489d";
        var study_name = "T-myawesomestudy";
        rowProperties.put(OBJECT_ID_KEY, expObjectId);
        rowProperties.put(STUDY_NAME_KEY, study_name);
        var row = new Metadata.Row();
        row.setProperties(rowProperties);
        var filePath = generator.getMetadataFilePath(row);
        assertThat(filePath, equalTo(String.format("metadata-the_repo/%s-%s.json", expObjectId, study_name)));
    }

    @Test
    void getContentFilePath() {
        Map<String, Object> rowProperties = new HashMap<>();
        var expObjectId = "090192f28025489d";
        var study_name = "T-myawesomestudy";
        rowProperties.put(OBJECT_ID_KEY, expObjectId);
        rowProperties.put(STUDY_NAME_KEY, study_name);
        var row = new Metadata.Row();
        row.setProperties(rowProperties);
        var filePath = generator.getContentFilePath(row);
        assertThat(filePath, equalTo(String.format("files-the_repo/%s-%s.pdf", expObjectId, study_name)));
    }

    @Test
    void noRepository() {
        var e = assertThrows(InvalidConfigurationException.class,
                () -> new FilenameGenerator(new Configs()));
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.sharedoc.repository'. Current value: ''. Please check your configuration."));
    }
}