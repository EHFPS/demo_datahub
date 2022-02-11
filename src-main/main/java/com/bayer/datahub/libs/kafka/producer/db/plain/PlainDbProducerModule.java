package com.bayer.datahub.libs.kafka.producer.db.plain;

import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryModule;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule;
import com.bayer.datahub.libs.services.dbcontext.DbContextModule;
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryServiceModule;
import com.google.inject.AbstractModule;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

import static com.bayer.datahub.libs.kafka.Serializers.AVRO_SERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.STRING_SERIALIZER;

public class PlainDbProducerModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new SchemaRegistryServiceModule());
        install(new DbContextModule());
        install(new ProducerFactoryModule());
        install(new ConsumerFactoryModule());
        bind(KafkaClient.class).to(PlainDbProducer.class);
        bind(ProducerFactoryConfigDefaults.class).toInstance(new ProducerFactoryConfigDefaults(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER)));
        bind(ConsumerFactoryConfigDefaults.class).toInstance(new ConsumerFactoryConfigDefaults(Map.of()));
    }
}
