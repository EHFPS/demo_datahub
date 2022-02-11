package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryModule;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryConfigDefaults;
import com.bayer.datahub.libs.kafka.producer.factory.ProducerFactoryModule;
import com.bayer.datahub.libs.services.dbcontext.DbContextModule;
import com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryServiceModule;
import com.google.inject.AbstractModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.AVRO_SERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.STRING_SERIALIZER;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.AUTO_OFFSET_RESET_LATEST;
import static com.bayer.datahub.libs.kafka.consumer.factory.ConsumerFactoryConfigDefaults.ENABLE_AUTO_COMMIT_TRUE;

public class DeltaDbProducerModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new SchemaRegistryServiceModule());
        install(new DbContextModule());
        install(new ProducerFactoryModule());
        install(new ConsumerFactoryModule());
        bind(KafkaClient.class).to(DeltaDbProducer.class);
        bind(ConsumerFactoryConfigDefaults.class).toInstance(new ConsumerFactoryConfigDefaults(Map.of(
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_TRUE,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AUTO_OFFSET_RESET_LATEST,
                ConsumerConfig.GROUP_ID_CONFIG, getClass().getSimpleName() + "-group",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AVRO_DESERIALIZER)));
        bind(ProducerFactoryConfigDefaults.class).toInstance(new ProducerFactoryConfigDefaults(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER)));
    }
}
