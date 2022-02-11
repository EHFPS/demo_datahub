package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import org.apache.commons.lang3.StringUtils;

import javax.inject.Inject;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_SHAREDOC_REPOSITORY_PROPERTY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.OBJECT_ID_KEY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.STUDY_NAME_KEY;

class FilenameGenerator {
    public static final String FILENAME_KAFKA_HEADER = "filename";
    private final String repository;

    @Inject
    FilenameGenerator(Configs configs) {
        repository = configs.producerSharedocRepository;
        if (StringUtils.isBlank(repository)) {
            throw new InvalidConfigurationException(PRODUCER_SHAREDOC_REPOSITORY_PROPERTY, configs.producerSharedocRepository);
        }
    }

    public String getMetadataFilePath(Metadata.Row row) {
        return getFilePath(row, "metadata", "json");
    }

    public String getContentFilePath(Metadata.Row row) {
        return getFilePath(row, "files", "pdf");
    }

    private String getFilePath(Metadata.Row row, String dirPrefix, String extension) {
        var objectId = row.getProperties().get(OBJECT_ID_KEY).toString();
        var studyId = row.getProperties().get(STUDY_NAME_KEY).toString();
        return String.format("%s-%s/%s-%s.%s", dirPrefix, repository, objectId, studyId, extension);
    }
}
