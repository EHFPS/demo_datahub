package com.bayer.datahub.libs.kafka.consumer.cloud.gcs;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumerConfig;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudClientFactory;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudConsumer;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudConsumerConfig;
import com.bayer.datahub.libs.kafka.consumer.cloud.CloudFileUploader;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryModule;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_GCS_FORMAT_AVRO_OBJECT_NAME_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_GCS_FORMAT_CSV_OBJECT_NAME_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_GCS_OUTPUT_DIR_PROPERTY;
import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.BYTE_ARRAY_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.AUTO_OFFSET_RESET_EARLIEST;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.ENABLE_AUTO_COMMIT_FALSE;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class GcsConsumerModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new ConsumerFactoryModule());
        bind(KafkaClient.class).to(CloudConsumer.class);
        bind(CloudFileUploader.class).to(GcsFileUploader.class);
        bind(CloudClientFactory.class).to(GcsCloudClientFactoryImpl.class);
    }

    @Provides
    @SuppressWarnings("unused")
    public ConsumerFactoryConfigDefaults consumerFactoryConfigDefaults(Configs configs) {
        var map = new HashMap<String, Object>();
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_FALSE);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        if (configs.consumerGcsFormatType == FileFormat.BINARY) {
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BYTE_ARRAY_DESERIALIZER);
        } else {
            map.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
            map.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AVRO_DESERIALIZER);
        }
        return new ConsumerFactoryConfigDefaults(map);
    }

    @Provides
    @SuppressWarnings("unused")
    public CloudConsumerConfig cloudConsumerConfig(Configs configs) {
        if (configs.consumerGcsFormatType == FileFormat.CSV && isBlank(configs.consumerGcsFormatCsvObjectName)) {
            throw new InvalidConfigurationException(CONSUMER_GCS_FORMAT_CSV_OBJECT_NAME_PROPERTY, configs.consumerGcsFormatCsvObjectName);
        }
        if (configs.consumerGcsFormatType == FileFormat.AVRO && isBlank(configs.consumerGcsFormatAvroObjectName)) {
            throw new InvalidConfigurationException(CONSUMER_GCS_FORMAT_AVRO_OBJECT_NAME_PROPERTY, configs.consumerGcsFormatAvroObjectName);
        }
        if (isBlank(configs.consumerGcsOutputDir)) {
            throw new InvalidConfigurationException(CONSUMER_GCS_OUTPUT_DIR_PROPERTY, configs.consumerGcsOutputDir);
        }
        return new CloudConsumerConfig(
                configs.kafkaCommonTopic,
                configs.consumerGcsFormatType == FileFormat.CSV ? configs.consumerGcsFormatCsvObjectName : configs.consumerGcsFormatAvroObjectName,
                configs.consumerGcsFormatType, configs.consumerGcsRetryMaxNumber, configs.consumerGcsRetryWaitTimeMs,
                configs.consumerGcsOutputDir, configs.consumerGcsFormatBinaryHeaderName,
                configs.consumerGcsFormatCsvValueDelimiter, configs.consumerGcsFormatCsvRecordDelimiter,
                configs.consumerGcsFormatCsvCharset
        );
    }

    @Provides
    @SuppressWarnings("unused")
    public RecordConsumerConfig recordConsumerConfig(Configs configs) {
        return RecordConsumerConfig.newInstance(configs, configs.consumerGcsGuarantee, configs.consumerGcsPollTimeoutMs);
    }

}
