package com.bayer.datahub.libs.kafka.consumer.splunk;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.consumer.RecordConsumerConfig;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Map;

import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.AUTO_OFFSET_RESET_EARLIEST;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.ENABLE_AUTO_COMMIT_FALSE;

public class SplunkConsumerModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new ConsumerFactoryModule());
        bind(KafkaClient.class).to(SplunkConsumer.class);
        bind(ConsumerFactoryConfigDefaults.class).toInstance(new ConsumerFactoryConfigDefaults(Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_FALSE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AVRO_DESERIALIZER)));
    }

    @Provides
    @SuppressWarnings("unused")
    public RecordConsumerConfig recordConsumerConfig(Configs configs) {
        return RecordConsumerConfig.newInstance(configs, configs.consumerBaseGuarantee, configs.consumerBasePollTimeoutMs);
    }
}
