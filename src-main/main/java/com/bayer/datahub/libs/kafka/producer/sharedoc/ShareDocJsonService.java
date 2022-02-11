package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.services.json.JsonService;

import javax.inject.Inject;

class ShareDocJsonService {
    private final JsonService jsonService;

    @Inject
    ShareDocJsonService(JsonService jsonService) {
        this.jsonService = jsonService;
    }

    public Metadata deserializeMetadata(byte[] metadata) {
        return jsonService.readValue(metadata, Metadata.class);
    }

    public String serializeMetadata(Metadata metadata) {
        return jsonService.writeValue(metadata);
    }

    public String serializeMetadataRowProperties(Metadata.Row metadataRow) {
        return jsonService.writeValue(metadataRow.getProperties());
    }

    public Metadata.Row deserializeMetadataRowProperties(String metadataRow) {
        return jsonService.readValue(metadataRow, Metadata.Row.class);
    }
}
