package com.bayer.datahub.libs.kafka.consumer.es;

import org.elasticsearch.client.RestClient;

public interface EsClientFactory {
    RestClient getRestClient();
}
