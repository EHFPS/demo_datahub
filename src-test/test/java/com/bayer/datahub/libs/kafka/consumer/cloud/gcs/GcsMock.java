package com.bayer.datahub.libs.kafka.consumer.cloud.gcs;

import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.contrib.nio.testing.LocalStorageHelper;
import org.apache.commons.lang3.concurrent.ConcurrentException;
import org.apache.commons.lang3.concurrent.ConcurrentInitializer;
import org.apache.commons.lang3.concurrent.LazyInitializer;

public class GcsMock {
    private static final ConcurrentInitializer<Storage> initializer = new LazyInitializer<>() {
        @Override
        protected Storage initialize() {
            return LocalStorageHelper.getOptions().getService();
        }
    };

    public Storage getGcsStorage() {
        try {
            return initializer.get();
        } catch (ConcurrentException e) {
            throw new RuntimeException(e);
        }
    }

    public CloudClientFactory getGcsCloudClientFactory() {
        return new CloudClientFactory() {
            @Override
            public <T> T createClient() {
                return (T) getGcsStorage();
            }
        };
    }
}