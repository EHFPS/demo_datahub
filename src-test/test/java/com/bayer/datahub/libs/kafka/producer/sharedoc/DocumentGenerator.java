package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.ResourceHelper;
import com.bayer.datahub.libs.services.json.JsonService;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.MODIFY_DATE_KEY;
import static com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocService.OBJECT_ID_KEY;

/**
 * Generates fake ShareDoc Documents for unit-tests.
 */
class DocumentGenerator {
    private static final Random r = new Random();
    private static final ShareDocJsonService SHARE_DOC_JSON_SERVICE = new ShareDocJsonService(new JsonService());
    private static final Metadata metadataTemplate = SHARE_DOC_JSON_SERVICE.deserializeMetadata(
            ResourceHelper.resourceToBytes(DocumentGenerator.class,
                    "DocumentGenerator_metadata_template.json"));
    private static final Metadata.Row metadataRowTemplate = SHARE_DOC_JSON_SERVICE.deserializeMetadataRowProperties(
            ResourceHelper.resourceToString(DocumentGenerator.class,
                    "DocumentGenerator_metadata_row_template.json"));
    private static final Instant now = Instant.parse("2020-09-21T08:11:59Z");

    Doc createNewDoc(int contentSize) {
        var docId = "doc-id-" + r.nextInt(Integer.MAX_VALUE);
        Metadata.Row metadata = generateMetadataRow(docId);
        byte[] content = generateContent(docId, contentSize);
        return new Doc(docId, metadata, content);
    }

    private Metadata.Row generateMetadataRow(String docId) {
        Metadata.Row metadata = metadataRowTemplate.clone();
        metadata.getProperties().put(OBJECT_ID_KEY, docId);
        var hours = r.nextInt(24 * 7 * 52);
        var modifyDate = now.minus(hours, ChronoUnit.HOURS).toString();
        metadata.getProperties().put(MODIFY_DATE_KEY, modifyDate);
        return metadata;
    }

    private byte[] generateContent(String docId, int contentSize) {
        var repeatNum = contentSize / docId.length() + 1;
        var contentStr = docId.repeat(repeatNum);
        var contentBytes = contentStr.getBytes();
        return Arrays.copyOf(contentBytes, contentSize);
    }

    Metadata mergeMetadataToSingleJson(List<Metadata.Row> metadataRowList) {
        var metadata = metadataTemplate.clone();
        metadata.setRows(metadataRowList);
        metadata.setTotalLength(metadataRowList.size());
        return metadata;
    }

    int generateDocSize(double zeroDocRation, double bigDocRatio, int chunkSize) {
        if (r.nextDouble() < zeroDocRation) {
            return 0;
        } else if (r.nextDouble() < bigDocRatio) {
            return r.nextInt(chunkSize * 5) + chunkSize;
        } else {
            return r.nextInt(chunkSize - 2) + 2;//2 because of single UTF-8 character
        }
    }

    static final class Doc {
        public final String id;
        public final Metadata.Row metadataRow;
        public final byte[] content;

        Doc(String id, Metadata.Row metadataRow, byte[] content) {
            this.id = id;
            this.metadataRow = metadataRow;
            this.content = content;
        }
    }
}
