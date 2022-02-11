package com.bayer.datahub.libs.kafka.consumer.cloud;

public interface CloudClientFactory {
    <T> T createClient();
}
