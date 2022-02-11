package com.bayer.datahub.libs.kafka.consumer;

import com.bayer.datahub.libs.config.Configs;

import static com.bayer.datahub.libs.config.Configs.CONSUMER_BASE_GUARANTEE_DEFAULT;

public class RecordConsumerConfig {
    public final String topic;
    public final long offset;
    public final int partition;
    public final ConsumerGuarantee guarantee;
    public final long pollTimeoutMs;
    public final boolean exitOnFinish;
    public final boolean includeMetadata;
    public final String deserializerProperty;

    private RecordConsumerConfig(String topic,
                                 long offset,
                                 int partition,
                                 ConsumerGuarantee guarantee,
                                 long pollTimeoutMs,
                                 boolean exitOnFinish,
                                 boolean includeMetadata,
                                 String deserializerProperty) {
        this.topic = topic;
        this.offset = offset;
        this.partition = partition;
        this.guarantee = guarantee;
        this.pollTimeoutMs = pollTimeoutMs;
        this.exitOnFinish = exitOnFinish;
        this.includeMetadata = includeMetadata;
        this.deserializerProperty = deserializerProperty;
    }

    public static RecordConsumerConfig newInstance(Configs configs, String guarantee, long pollTimeoutMsProperty) {
        return new RecordConsumerConfig(
                configs.kafkaCommonTopic,
                configs.kafkaConsumerOffset,
                configs.kafkaConsumerPartition,
                ConsumerGuarantee.parse(guarantee, CONSUMER_BASE_GUARANTEE_DEFAULT),
                pollTimeoutMsProperty,
                configs.kafkaConsumerExitOnFinish,
                configs.includeMetadata,
                configs.deserializerProperty
        );
    }
}
