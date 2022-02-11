package com.bayer.datahub.libs.kafka.consumer.cloud.gcs;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudFileUploader;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_GCS_GCS_BUCKET_NAME_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GcsFileUploaderTest {
    private static final String BUCKET_NAME = ""; //GCS test Storage implementation doesn't support buckets ¯\_(ツ)_/¯
    private static final GcsMock gcsMock = new GcsMock();
    private static final Storage storage = gcsMock.getGcsStorage();
    private static final FactoryBuilder.Factory factory = FactoryBuilder.newBuilder(Map.of(
            CLIENT_TYPE_PROPERTY, ClientType.CONSUMER_GCS.name(),
            CONSUMER_GCS_GCS_BUCKET_NAME_PROPERTY, BUCKET_NAME))
            .override(CloudClientFactory.class, gcsMock.getGcsCloudClientFactory())
            .withSchemaRegistryMock()
            .build();

    private static void assertBlob(String objectName, String expContent) {
        var blob2 = storage.get(BlobId.of(BUCKET_NAME, objectName));
        if (blob2 == null) {
            throw new AssertionError("Blob not found by name: " + objectName);
        }
        var os = new ByteArrayOutputStream();
        blob2.downloadTo(os);
        assertThat(os.toString(), equalTo(expContent));
    }

    @Test
    void uploadDir() {
        var dir = Paths.get(getClass().getResource("upload_dir").getFile());
        var uploader = factory.getInstance(CloudFileUploader.class);
        uploader.uploadDir(dir);

        assertBlob("file1.txt", "the_file_1");
        assertBlob("subdir1/file2.txt", "the_file_2");
        assertBlob("subdir1/file3.txt", "the_file_3");
        assertBlob("subdir1/subdir2/file4.txt", "the_file_4");
    }

    @Test
    void uploadDirEmpty() throws IOException {
        var emptyDir = Files.createTempDirectory(getClass().getSimpleName());
        var uploader = factory.getInstance(CloudFileUploader.class);
        uploader.uploadDir(emptyDir);
    }

    @Test
    void uploadDirNotExists() {
        var dirName = "/not_exists_dir";
        var notExistsDir = Paths.get(dirName);
        var uploader = factory.getInstance(CloudFileUploader.class);
        var e = assertThrows(RuntimeException.class, () -> uploader.uploadDir(notExistsDir));
        var cause = e.getCause();
        assertThat(cause, instanceOf(NoSuchFileException.class));
        assertThat(cause.getMessage(), equalTo(dirName));
    }

}