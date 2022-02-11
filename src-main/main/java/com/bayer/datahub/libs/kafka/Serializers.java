package com.bayer.datahub.libs.kafka;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;

/**
 * Holds some constant string values to pass into serializer configs for a Kafka producer
 */
public class Serializers {
    public static final String STRING_SERIALIZER = StringSerializer.class.getName();
    public static final String AVRO_SERIALIZER = KafkaAvroSerializer.class.getName();
    public static final String BYTE_ARRAY_SERIALIZER = ByteArraySerializer.class.getName();

    public static Map<String, String> STRING_STRING_SERIALIZER = Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);

    public static Map<String, String> STRING_BYTES_SERIALIZER = Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BYTE_ARRAY_SERIALIZER);

    public static Map<String, String> STRING_AVRO_SERIALIZER = Map.of(
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AVRO_SERIALIZER);
}
