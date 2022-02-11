package com.bayer.datahub.libs.kafka.consumer.es;

import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class EsConverterTest {

    @Test
    void convertAvroSchemaToEsMapping() {
        var schema = SchemaBuilder.record("the_record").namespace("the_namespace")
                .fields()
                .name("model").type().stringType().noDefault()
                .name("year").type().doubleType().noDefault()
                .name("changed").type().stringType().noDefault()
                .endRecord();
        var converter = new EsConverter();
        var actEsMapping = converter.convertAvroSchemaToEsMapping(schema);
        var expEsMapping = "{\"dynamic\":\"true\",\"numeric_detection\":true,\"properties\":{\"model\":{\"type\":\"keyword\",\"ignore_above\":256,\"norms\":false,\"index_options\":\"freqs\"},\"year\":{\"type\":\"double\"},\"changed\":{\"type\":\"keyword\",\"ignore_above\":256,\"norms\":false,\"index_options\":\"freqs\"}}}";
        assertThat(actEsMapping, equalTo(expEsMapping));
    }
}