package com.bayer.datahub.libs.kafka.consumer.splunk;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.Consumer;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumer;
import com.bayer.datahub.libs.services.restapi.SplunkHecWriter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * An implementation of the {@link Consumer} interface.
 * Can connect and write to a Splunk HTTP event collector (HEC).
 */
class SplunkConsumer implements Consumer {
    private static final Logger logger = LoggerFactory.getLogger(SplunkConsumer.class);
    private static final boolean USE_SSL = true;
    private static final boolean INDEXER_ACK = true;
    private static final boolean RAW_ENDPOINT = true;
    private static final boolean VERIFY_SSL = true;
    private static final int HEC_OK = 200;
    private final SplunkHecWriter hec;
    private final List<String> data = new LinkedList<>();
    private final Map<String, String> metaData = new HashMap<>();
    private final RecordConsumer recordConsumer;
    private final Configs configs;
    private long currentOffset;

    @Inject
    public SplunkConsumer(RecordConsumer recordConsumer, Configs configs) {
        this.recordConsumer = recordConsumer;
        this.configs = configs;
        var consumerSplunkHecPeers = Arrays.asList(configs.consumerSplunkHecPeers.split(","));
        var consumerSplunkHecUseSSL = configs.consumerSplunkHecUseSSL.isEmpty() ? USE_SSL : Boolean.parseBoolean(configs.consumerSplunkHecUseSSL);
        var consumerSplunkHecAuthToken = configs.consumerSplunkHecAuthToken;
        var consumerSplunkHecUseIndexerAck = configs.consumerSplunkHecUseIndexerAck.isBlank() ? INDEXER_ACK : Boolean.parseBoolean(configs.consumerSplunkHecUseIndexerAck);
        var consumerSplunkHecUseRawEndpoint = configs.consumerSplunkHecUseRawEndpoint.isBlank() ? RAW_ENDPOINT : Boolean.parseBoolean(configs.consumerSplunkHecUseRawEndpoint);
        var consumerSplunkHecRawLineBreaker = configs.consumerSplunkHecRawLineBreaker;
        var consumerSplunkHecKeystoreLocation = configs.consumerSplunkHecKeystoreLocation;
        var consumerSplunkHecKeystorePassword = configs.consumerSplunkHecKeystorePassword;
        var consumerSplunkHecVerifySSL = configs.consumerSplunkHecVerifySSL.isEmpty() ? VERIFY_SSL : Boolean.parseBoolean(configs.consumerSplunkHecVerifySSL);
        hec = new SplunkHecWriter(consumerSplunkHecPeers, consumerSplunkHecUseSSL, consumerSplunkHecAuthToken, consumerSplunkHecUseIndexerAck,
                consumerSplunkHecUseRawEndpoint, consumerSplunkHecRawLineBreaker, consumerSplunkHecKeystoreLocation, consumerSplunkHecKeystorePassword,
                consumerSplunkHecVerifySSL
        );
    }

    @Override
    public void init() {
        recordConsumer.init();
        if (!configs.consumerSplunkHecOverrideIndex.isEmpty()) {
            metaData.put("index", configs.consumerSplunkHecOverrideIndex);
        }
        if (!configs.consumerSplunkHecOverrideHost.isEmpty()) {
            metaData.put("host", configs.consumerSplunkHecOverrideHost);
        }
        if (!configs.consumerSplunkHecOverrideSource.isEmpty()) {
            metaData.put("source", configs.consumerSplunkHecOverrideSource);
        }
        if (!configs.consumerSplunkHecOverrideSourceType.isEmpty()) {
            metaData.put("sourcetype", configs.consumerSplunkHecOverrideSourceType);
        }
    }

    @Override
    public void run() {
        recordConsumer.run((records -> {
            if (records.isEmpty()) {
                return;
            }
            final CloseableHttpResponse hecResponse;
            var stringRecords = (ConsumerRecords<?, String>) records;
            for (ConsumerRecord<?, String> record : stringRecords) {
                data.add(record.value());
                currentOffset = record.offset();
            }
            try {
                hecResponse = hec.sendEvents(data, metaData);
                if (hecResponse.getStatusLine().getStatusCode() == HEC_OK) {
                    recordConsumer.commit();
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            recordConsumer.commit();
        }));
    }

    @Override
    public void stop() {
        recordConsumer.stop();
    }
}
