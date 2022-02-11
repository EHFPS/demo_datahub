package com.bayer.datahub.libs.kafka.consumer.file;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumerConfig;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryModule;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryServiceModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.HashMap;

import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.BYTE_ARRAY_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.AUTO_OFFSET_RESET_EARLIEST;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.ENABLE_AUTO_COMMIT_FALSE;

public class FileConsumerModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new ConsumerFactoryModule());
        install(new SchemaRegistryServiceModule());
        bind(KafkaClient.class).to(FileConsumer.class);
    }

    @Provides
    @SuppressWarnings("unused")
    public ConsumerFactoryConfigDefaults consumerFactoryConfigDefaults(Configs configs) {
        var map = new HashMap<String, Object>();
        map.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_FALSE);
        map.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST);
        if (configs.consumerFileFileFormat == FileFormat.BINARY) {
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
    public RecordConsumerConfig recordConsumerConfig(Configs configs) {
        return RecordConsumerConfig.newInstance(configs, configs.consumerBaseGuarantee, configs.consumerBasePollTimeoutMs);
    }

}
