package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryModule;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.bayer.datahub.libs.kafka.Deserializers.BYTE_ARRAY_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.BYTE_ARRAY_SERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.STRING_SERIALIZER;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.AUTO_OFFSET_RESET_EARLIEST;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.ENABLE_AUTO_COMMIT_TRUE;

public class ShareDocProducerModule extends AbstractModule {
    public static final String SHAREDOC_HTTP_CLIENT_NAME = "share-doc-http-client";
    private static final Logger log = LoggerFactory.getLogger(ShareDocProducerModule.class);

    @Override
    protected void configure() {
        install(new ProducerFactoryModule());
        install(new ConsumerFactoryModule());
        bind(KafkaClient.class).to(ShareDocProducer.class);
    }

    @Provides
    @Named(SHAREDOC_HTTP_CLIENT_NAME)
    @SuppressWarnings("unused")
    public HttpClient httpClient(Configs configs) {
        var timeout = Duration.ofSeconds(configs.producerSharedocHttpTimeoutSeconds);
        var parallelism = configs.producerSharedocParallelism;
        var executor = Executors.newFixedThreadPool(parallelism, runnable -> {
            var thread = new Thread(runnable);
            thread.setDaemon(true);
            return thread;
        });
        log.debug("HttpClient timeout: {}", timeout);
        return HttpClient.newBuilder().executor(executor).connectTimeout(timeout).build();
    }

    @Provides
    @SuppressWarnings("unused")
    public ConsumerFactoryConfigDefaults consumerFactoryConfigDefaults(Configs configs) {
        var valueDeserializer = Mode.parse(configs.producerSharedocMode) == Mode.METADATA ?
                STRING_DESERIALIZER : BYTE_ARRAY_DESERIALIZER;
        return new ConsumerFactoryConfigDefaults(Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_TRUE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_EARLIEST,
                ConsumerConfig.GROUP_ID_CONFIG, getClass().getSimpleName() + "-group",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer));
    }

    @Provides
    @SuppressWarnings("unused")
    public ProducerFactoryConfigDefaults producerFactoryConfigDefaults(Configs configs) {
        var valueDeserializer = Mode.parse(configs.producerSharedocMode) == Mode.METADATA ?
                STRING_SERIALIZER : BYTE_ARRAY_SERIALIZER;
        return new ProducerFactoryConfigDefaults(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueDeserializer));
    }
}
