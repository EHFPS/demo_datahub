package com.bayer.datahub.libs.kafka.consumer.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ElasticSearchTest {
    static final ElasticsearchContainer ELASTICSEARCH_CONTAINER;

    static {
        ELASTICSEARCH_CONTAINER = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch-oss:6.8.16"));
        ELASTICSEARCH_CONTAINER.start();
    }

    @Test
    public void indexDocument() throws IOException {
        var indexName = "test";
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(HttpHost.create(ELASTICSEARCH_CONTAINER.getHttpHostAddress())));
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        IndexRequest request = new IndexRequest(indexName);
        request.id("1");
        String jsonString = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
        RestStatus status = indexResponse.status();
        int statusCode = status.getStatus();
        MatcherAssert.assertThat(statusCode, equalTo(201));
        client.close();
    }

}