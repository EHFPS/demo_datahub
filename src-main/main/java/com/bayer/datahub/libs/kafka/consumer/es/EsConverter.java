package com.bayer.datahub.libs.kafka.consumer.es;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.avro.Schema.Type.INT;
import static org.apache.avro.Schema.Type.STRING;
import static org.apache.avro.Schema.Type.UNION;


class EsConverter {
    private static final Logger log = LoggerFactory.getLogger(EsConverter.class);
    private static final CsvMapper csvMapper = new CsvMapper();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static String avroDataTypeToES(String avroType) {
        if (avroType.equalsIgnoreCase(INT.name())) {
            avroType = "integer";
        } else if (avroType.equalsIgnoreCase(STRING.name())) {
            avroType = "keyword";
        }
        if (!StringUtils.containsAny(avroType.toLowerCase(), "integer", "long", "double", "float",
                "boolean", "keyword")) {
            return null;
        }
        return avroType;
    }

    public String convertAvroSchemaToEsMapping(Schema avroSchema) {
        try {
            var fields = avroSchema.getFields();
            var builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("dynamic", "true");
                builder.field("numeric_detection", true);
                builder.startObject("properties");
                {
                    for (var field : fields) {
                        var avroType = field.schema().getType().getName();
                        if (field.schema().getType() == UNION && field.schema().getTypes().size() > 1) {
                            avroType = field.schema().getTypes().get(1).getType().getName();
                        }
                        var esDataType = avroDataTypeToES(avroType);
                        if (esDataType == null) {
                            log.warn("Cannot convert Avro type '{}' for field '{}'. Skip it.", avroType, field);
                            continue;
                        }
                        builder.startObject(field.name());
                        {
                            builder.field("type", esDataType);
                            if (esDataType.equals("keyword")) {
                                //for performance tuning
                                builder.field("ignore_above", 256);
                                builder.field("norms", false);
                                builder.field("index_options", "freqs");
                            }
                        }
                        builder.endObject();
                    }
                }
                builder.endObject();
            }
            builder.endObject();
            builder.close();
            return Strings.toString(builder);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String csvToJson(String csv) {
        try {
            var csvSchema = CsvSchema.emptySchema().withHeader();
            var list = csvMapper.reader().forType(Map.class)
                    .with(csvSchema).<Map<?, ?>>readValues(csv)
                    .readAll();
            var os = new ByteArrayOutputStream();
            objectMapper.writeValue(os, list);
            return os.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
