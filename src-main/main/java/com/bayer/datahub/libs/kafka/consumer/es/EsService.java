package com.bayer.datahub.libs.kafka.consumer.es;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.kafka.RecordService;
import org.apache.avro.generic.GenericRecord;
import org.apache.http.HttpStatus;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.CheckedConsumer;
import org.elasticsearch.common.xcontent.XContentType;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

class EsService {
    private final EsClientFactory esClientFactory;
    private final RecordService recordService;
    private final EsConverter esConverter;
    private final String indexName;
    private RestHighLevelClient highLevelRestClient;

    @Inject
    public EsService(EsClientFactory esClientFactory, RecordService recordService, EsConverter esConverter, Configs configs) {
        this.esClientFactory = esClientFactory;
        this.recordService = recordService;
        this.esConverter = esConverter;
        indexName = configs.consumerEsIndexName;
    }

    public void init() {
        highLevelRestClient = instantiateHighLevelClient(esClientFactory.getRestClient());
        if (!isIndexExists()) {
            throw new RuntimeException("ES index does not exist: " + indexName);
        }
    }

    public void indexGenericRecord(GenericRecord record) {
        var source = recordService.convertGenericRecordToJson(record);
        indexSource(source);
    }

    public void indexCsv(String csv) {
        var source = esConverter.csvToJson(csv);
        indexSource(source);
    }

    public void indexJson(String json) {
        indexSource(json);
    }

    private void indexSource(String source) {
        try {
            var request = new IndexRequest(indexName);
            request.source(source, XContentType.JSON);
            var indexResponse = highLevelRestClient.index(request, RequestOptions.DEFAULT);
            var status = indexResponse.status();
            var statusCode = status.getStatus();
            if (statusCode != HttpStatus.SC_CREATED) {
                throw new IOException("Response status code: " + status);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            highLevelRestClient.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isIndexExists() {
        try {
            return highLevelRestClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * It's impossible to instantiate {@link RestHighLevelClient} from {@link RestClient}
     * ({@link RestClientBuilder} is required),  but {@link RestClientBuilder} is not available in ES Test Framework.
     */
    private RestHighLevelClient instantiateHighLevelClient(RestClient restClient) {
        try {
            var constructor = RestHighLevelClient.class.getDeclaredConstructor(
                    RestClient.class, CheckedConsumer.class, List.class);
            constructor.setAccessible(true);
            CheckedConsumer<RestClient, IOException> doClose = RestClient::close;
            return constructor.newInstance(restClient, doClose, Collections.emptyList());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
