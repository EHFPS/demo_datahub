package com.bayer.datahub.libs.kafka;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Holds some constant string values to pass into deserializer configs for a Kafka consumer
 */
public class Deserializers {
    public static final String STRING_DESERIALIZER = StringDeserializer.class.getName();
    public static final String AVRO_DESERIALIZER = KafkaAvroDeserializer.class.getName();
    public static final String BYTE_ARRAY_DESERIALIZER = ByteArrayDeserializer.class.getName();
}
