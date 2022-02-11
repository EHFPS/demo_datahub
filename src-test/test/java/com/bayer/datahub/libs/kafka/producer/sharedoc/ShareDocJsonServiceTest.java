package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.FactoryBuilder;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.bayer.datahub.ResourceHelper.resourceToBytes;
import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_SHAREDOC;
import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ShareDocJsonServiceTest {
    private final ShareDocJsonService shareDocJsonService = FactoryBuilder
            .newBuilder(Map.of(CLIENT_TYPE_PROPERTY, PRODUCER_SHAREDOC.name()))
            .build().getInstance(ShareDocJsonService.class);

    @Test
    void deserializeMetadata() {
        var bytes = resourceToBytes(ShareDocJsonServiceTest.class, "JsonServiceTest.json");
        var metadata = shareDocJsonService.deserializeMetadata(bytes);

        assertThat(metadata.getOffset(), equalTo(0));
        assertThat(metadata.getTotalLength(), equalTo(1));

        var columns = metadata.getColumns();
        assertThat(columns, hasSize(232));

        var column0 = columns.get(0);
        assertThat(column0.getName(), equalTo("r_folder_path"));
        assertThat(column0.getLabel(), equalTo("Folder path"));

        var rows = metadata.getRows();
        assertThat(rows, hasSize(1));

        var row0 = rows.get(0);
        assertThat(row0.getClazz(), equalTo("model"));
        assertThat(row0.getModifiedProperties(), empty());

        var properties = row0.getProperties();
        assertThat(properties, aMapWithSize(212));
        assertThat(properties, hasEntry("a_content_type", "pdf"));
        assertThat(properties, hasEntry("a_full_text", 1));

    }
}