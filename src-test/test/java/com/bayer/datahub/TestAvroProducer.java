package com.bayer.datahub;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryService.SCHEMA_REGISTRY_CONFIG;

/**
 * Produces one Avro record to a topic (for manual testing purposes).
 */
class TestAvroProducer {
    private static final String MODEL_FIELD = "MODEL";
    private static final String YEAR_FIELD = "YEAR";
    private static final String CHANGED_FIELD = "CHANGED";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        var propertiesFile = "c:/alex/files/kafka_client_cert/kafka_client_cert-iablokov-non-prod-20200512112033/TestAvroProducer.properties";

        var properties = new Properties();
        properties.load(new FileInputStream(propertiesFile));
        var topic = (String) properties.remove("topic");

        var now = LocalDateTime.now().toString();
        var schema = loadSchema(properties, topic);
        var record = new GenericData.Record(schema);
        record.put(MODEL_FIELD, "Reno");
        record.put(YEAR_FIELD, 2015D);
        record.put(CHANGED_FIELD, now);

        var key = "key-" + now;

        var producerRecord = new ProducerRecord<String, Object>(topic, 0, key, record);

        try (var producer = new KafkaProducer<String, Object>(properties)) {
            producer.send(producerRecord).get();
            System.out.printf("A record was produced:\ntopic='%s'\nkey='%s'\nschema='%s'\nrecord='%s'\n",
                    topic, key, schema, record);
        }
    }

    private static Schema loadSchema(Properties properties, String topic) throws IOException {
        Schema valueSchema;
        try {
            var schemaRegistryUrl = (String) properties.get(SCHEMA_REGISTRY_CONFIG);
            var client = new CachedSchemaRegistryClient(schemaRegistryUrl, 100);
            var schemaMetadata = client.getLatestSchemaMetadata(topic + "-value");
            var schemaId = schemaMetadata.getId();
            valueSchema = client.getByID(schemaId);
        } catch (RestClientException e) {
            valueSchema = SchemaBuilder.record("the_record").namespace("the_namespace")
                    .fields()
                    .name(MODEL_FIELD).type().stringType().noDefault()
                    .name(YEAR_FIELD).type().doubleType().noDefault()
                    .name(CHANGED_FIELD).type().stringType().noDefault()
                    .endRecord();
        }
        return valueSchema;
    }
}
