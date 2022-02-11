package com.bayer.datahub.libs.kafka.consumer.cloud.gcs;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudFileUploader;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_GCS_GCS_BUCKET_NAME_PROPERTY;

class GcsFileUploader implements CloudFileUploader {
    private static final Logger log = LoggerFactory.getLogger(GcsFileUploader.class);
    private final CloudClientFactory clientFactory;
    private final String bucketName;

    @Inject
    GcsFileUploader(CloudClientFactory clientFactory, Configs configs) {
        this.clientFactory = clientFactory;
        this.bucketName = configs.consumerGcsGcsBucketName;
    }

    @Override
    public void checkCredentials() {
        log.debug("Checking Google Cloud Storage credentials...");
        Storage client = clientFactory.createClient();
        if (Strings.isNullOrEmpty(bucketName)) {
            throw new InvalidConfigurationException(CONSUMER_GCS_GCS_BUCKET_NAME_PROPERTY, bucketName);
        }
        var bucket = client.get(bucketName);
        if (bucket == null) {
            throw new IllegalStateException("Bucket not found: " + bucketName);
        }
        log.debug("Google Cloud Storage credentials are correct.");
    }

    @Override
    public void uploadDir(Path dir) {
        try {
            Storage storage = clientFactory.createClient();
            var visitor = new FileVisitor(storage, bucketName, dir);
            Files.walkFileTree(dir, visitor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
    }

    private static class FileVisitor extends SimpleFileVisitor<Path> {
        private final Storage storage;
        private final String bucketName;
        private final Path rootPath;

        private FileVisitor(Storage storage, String bucketName, Path rootPath) {
            this.storage = storage;
            this.bucketName = bucketName;
            this.rootPath = rootPath;
        }

        private static Path getCommonEnd(Path dir, Path fileInDir) {
            var p1 = Objects.requireNonNull(dir);
            var p2 = Objects.requireNonNull(fileInDir);
            if (p1.equals(p2)) {
                return p1;
            } else if (p1.startsWith(p2)) {
                return Paths.get(p1.toString().substring(p2.toString().length() + 1));
            } else if (p2.startsWith(p1)) {
                return Paths.get(p2.toString().substring(p1.toString().length() + 1));
            } else {
                throw new IllegalArgumentException("File (" + fileInDir + ") is not located in the dir (" + dir + ")");
            }
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            try {
                var objectName = getCommonEnd(rootPath, file).toString().replace("\\", "/");
                var blobId = BlobId.of(bucketName, objectName);
                var blobInfo = BlobInfo.newBuilder(blobId).build();
                storage.createFrom(blobInfo, new FileInputStream(file.toFile()));
                log.debug("File uploaded to GCS: local='{}', bucket='{}', name='{}'", file, blobInfo.getBucket(), blobInfo.getName());
                return FileVisitResult.CONTINUE;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
