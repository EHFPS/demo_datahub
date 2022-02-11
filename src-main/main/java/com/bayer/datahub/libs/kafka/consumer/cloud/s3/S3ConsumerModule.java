package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

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
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;

import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_S3_FORMAT_AVRO_S3_KEY_NAME_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_S3_FORMAT_CSV_S3_KEY_NAME_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.CONSUMER_S3_OUTPUT_DIR_PROPERTY;
import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.BYTE_ARRAY_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.AUTO_OFFSET_RESET_EARLIEST;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.ENABLE_AUTO_COMMIT_TRUE;
import static org.apache.commons.lang3.StringUtils.isBlank;

public class S3ConsumerModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new ConsumerFactoryModule());
        install(new SchemaRegistryServiceModule());
        bind(KafkaClient.class).to(CloudConsumer.class);
        bind(CloudClientFactory.class).to(S3CloudClientFactoryImpl.class);
        bind(CloudFileUploader.class).to(S3FileUploader.class);
    }

    @Provides
    @SuppressWarnings("unused")
    public ConsumerFactoryConfigDefaults consumerFactoryConfigDefaults(Configs configs) {
        var map = new HashMap<String, Object>();
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_TRUE);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        if (configs.consumerS3FormatType == FileFormat.BINARY) {
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
        if (configs.consumerS3FormatType == FileFormat.CSV && isBlank(configs.consumerS3FormatCsvS3KeyName)) {
            throw new InvalidConfigurationException(CONSUMER_S3_FORMAT_CSV_S3_KEY_NAME_PROPERTY, configs.consumerS3FormatCsvS3KeyName);
        }
        if (configs.consumerS3FormatType == FileFormat.AVRO && isBlank(configs.consumerS3FormatAvroS3KeyName)) {
            throw new InvalidConfigurationException(CONSUMER_S3_FORMAT_AVRO_S3_KEY_NAME_PROPERTY, configs.consumerS3FormatAvroS3KeyName);
        }
        if (isBlank(configs.consumerS3OutputDir)) {
            throw new InvalidConfigurationException(CONSUMER_S3_OUTPUT_DIR_PROPERTY, configs.consumerS3OutputDir);
        }
        return new CloudConsumerConfig(
                configs.kafkaCommonTopic,
                configs.consumerS3FormatType == FileFormat.CSV ? configs.consumerS3FormatCsvS3KeyName : configs.consumerS3FormatAvroS3KeyName,
                configs.consumerS3FormatType, configs.consumerS3RetryMaxNumber, configs.consumerS3RetryWaitTimeMs,
                configs.consumerS3OutputDir, configs.consumerS3FormatBinaryHeaderName,
                configs.consumerS3FormatCsvValueDelimiter, configs.consumerS3FormatCsvRecordDelimiter,
                configs.consumerS3FormatCsvCharset
        );
    }

    @Provides
    @SuppressWarnings("unused")
    public RecordConsumerConfig recordConsumerConfig(Configs configs) {
        return RecordConsumerConfig.newInstance(configs, configs.consumerBaseGuarantee, configs.consumerBasePollTimeoutMs);
    }
}
