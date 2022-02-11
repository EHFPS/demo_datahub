package com.bayer.datahub.libs.kafka.producer.csv;

import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule;
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryServiceModule;
import com.google.inject.AbstractModule;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

import static com.bayer.datahub.libs.kafka.Serializers.AVRO_SERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.STRING_SERIALIZER;

public class CsvProducerModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new SchemaRegistryServiceModule());
        install(new ProducerFactoryModule());
        bind(KafkaClient.class).to(CsvProducer.class);
        bind(ProducerFactoryConfigDefaults.class).toInstance(new ProducerFactoryConfigDefaults(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER)));
    }
}
