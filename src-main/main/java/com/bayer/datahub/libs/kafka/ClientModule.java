package com.bayer.datahub.libs.kafka;

import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.kafka.consumer.bigquery.BigQueryConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.bigquery.BqSltConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.cloud.gcs.GcsConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3ConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.db.DbConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.es.EsConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.file.FileConsumerModule;
import com.bayer.datahub.libs.kafka.consumer.splunk.SplunkConsumerModule;
import com.bayer.datahub.libs.kafka.producer.csv.CsvProducerModule;
import com.bayer.datahub.libs.kafka.producer.db.delta.DeltaDbProducerModule;
import com.bayer.datahub.libs.kafka.producer.db.plain.PlainDbProducerModule;
import com.bayer.datahub.libs.kafka.producer.sharedoc.ShareDocProducerModule;
import com.google.inject.AbstractModule;

import static com.bayer.datahub.kafkaclientbuilder.ClientType.parse;
import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;

public class ClientModule extends AbstractModule {
    private final Configs configs;

    public ClientModule(Configs configs) {
        this.configs = configs;
    }

    @Override
    protected void configure() {
        bind(Configs.class).toInstance(configs);
        var clientType = parse(configs.clientType);
        switch (clientType) {
            case PRODUCER_DB_DELTA:
                install(new DeltaDbProducerModule());
                break;
            case PRODUCER_DB_PLAIN:
                install(new PlainDbProducerModule());
                break;
            case PRODUCER_SHAREDOC:
                install(new ShareDocProducerModule());
                break;
            case PRODUCER_CSV:
                install(new CsvProducerModule());
                break;
            case CONSUMER_S3:
                install(new S3ConsumerModule());
                break;
            case CONSUMER_FILE:
                install(new FileConsumerModule());
                break;
            case CONSUMER_DB:
                install(new DbConsumerModule());
                break;
            case CONSUMER_SPLUNK:
                install(new SplunkConsumerModule());
                break;
            case CONSUMER_BIGQUERY:
                install(new BigQueryConsumerModule());
                break;
            case CONSUMER_BQ_SLT:
                install(new BqSltConsumerModule());
                break;
            case CONSUMER_GCS:
                install(new GcsConsumerModule());
                break;
            case CONSUMER_ES:
                install(new EsConsumerModule());
                break;
            default:
                throw new InvalidConfigurationException(CLIENT_TYPE_PROPERTY, configs.clientType, ClientType.values());
        }
    }
}
