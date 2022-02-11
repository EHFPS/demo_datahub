package com.bayer.datahub.libs.config;

import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3AuthType;
import com.bayer.datahub.libs.kafka.producer.db.delta.DeltaType;
import com.bayer.datahub.libs.services.common.statistics.Statistics;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;

import static com.bayer.datahub.ResourceHelper.resourceToFile;
import static com.bayer.datahub.ResourceHelper.resourceToString;
import static com.bayer.datahub.libs.config.Configs.*;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.AT_LEAST_ONCE;
import static com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee.EXACTLY_ONCE;
import static com.bayer.datahub.libs.services.fileio.FileFormat.AVRO;
import static com.bayer.datahub.libs.services.fileio.FileFormat.CSV;
import static com.bayer.datahub.libs.services.schemaregistry.SchemaRegistryService.SCHEMA_REGISTRY_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.*;
import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.apache.kafka.common.config.SslConfigs.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ConfigsTest {

    private static Configs loadConfigFromResource(String s) {
        return loadConfigListFromResource(s).get(0);
    }

    private static ConfigsList loadConfigListFromResource(String s) {
        var configFile = resourceToFile(ConfigsTest.class, s);
        return ConfigFileLoader.readConfigsListFromFile(configFile);
    }

    private static void assertClientType(Configs configs, ClientType expClientType) {
        assertThat(ClientType.parse(configs.clientType), equalTo(expClientType));
    }

    private static void assertKafka(Configs configs) {
        assertThat(configs.kafkaCommonTopic, equalTo("the-topic"));
        assertThat(configs.kafkaCommonSchemaRegistryUrl, equalTo("http://sr.awseuc1.tst.edh.cnb:8081"));
        assertThat(configs.kafkaConsumerOffset, equalTo(-1L));
        assertThat(configs.kafkaConsumerPartition, equalTo(-1));
        var consumerProps = configs.kafkaConsumerProperties;
        assertThat(consumerProps, hasEntry(KEY_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer"));
        assertThat(consumerProps, hasEntry(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"));
        assertThat(consumerProps, hasEntry(REQUEST_TIMEOUT_MS_CONFIG, "30000"));
        assertThat(consumerProps, hasEntry(BOOTSTRAP_SERVERS_CONFIG, "kfk.awseuc1.tst.edh.cnb:9093"));
        assertThat(consumerProps, hasEntry(GROUP_ID_CONFIG, "regprem-dev"));
        assertThat(consumerProps, hasEntry(MAX_POLL_INTERVAL_MS_CONFIG, "5000"));
        assertThat(consumerProps, hasEntry(SECURITY_PROTOCOL_CONFIG, "SSL"));
        assertThat(consumerProps, hasEntry(SSL_KEY_PASSWORD_CONFIG, "THE_KEY_PASS"));
        assertThat(consumerProps, hasEntry(SSL_KEYSTORE_LOCATION_CONFIG, "c:/jks/kafka_client_keystore.jks"));
        assertThat(consumerProps, hasEntry(SSL_KEYSTORE_PASSWORD_CONFIG, "THE_KEYSTORE_PASS"));
        assertThat(consumerProps, hasEntry(SSL_TRUSTSTORE_LOCATION_CONFIG, "c:/jks/kafka_client_truststore.jks"));
        assertThat(consumerProps, hasEntry(SSL_TRUSTSTORE_PASSWORD_CONFIG, "THE_TRUSTSTORE_PASS"));
        assertThat(consumerProps, hasEntry(SCHEMA_REGISTRY_CONFIG, "http://sr.awseuc1.tst.edh.cnb:8081"));
        assertThat(consumerProps, aMapWithSize(15));

        var producerProps = configs.kafkaProducerProperties;
        assertThat(producerProps, hasEntry(KEY_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer"));
        assertThat(producerProps, hasEntry(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"));
        assertThat(producerProps, hasEntry(REQUEST_TIMEOUT_MS_CONFIG, "30000"));
        assertThat(producerProps, hasEntry(ACKS_CONFIG, "1"));
        assertThat(producerProps, hasEntry(DELIVERY_TIMEOUT_MS_CONFIG, "120000"));
        assertThat(producerProps, hasEntry(BOOTSTRAP_SERVERS_CONFIG, "kfk.awseuc1.tst.edh.cnb:9093"));
        assertThat(producerProps, hasEntry(SECURITY_PROTOCOL_CONFIG, "SSL"));
        assertThat(producerProps, hasEntry(MAX_REQUEST_SIZE_CONFIG, "1048576"));
        assertThat(producerProps, hasEntry(SSL_KEY_PASSWORD_CONFIG, "THE_KEY_PASS"));
        assertThat(producerProps, hasEntry(SSL_KEYSTORE_LOCATION_CONFIG, "c:/jks/kafka_client_keystore.jks"));
        assertThat(producerProps, hasEntry(SSL_KEYSTORE_PASSWORD_CONFIG, "THE_KEYSTORE_PASS"));
        assertThat(producerProps, hasEntry(SSL_TRUSTSTORE_LOCATION_CONFIG, "c:/jks/kafka_client_truststore.jks"));
        assertThat(producerProps, hasEntry(SSL_TRUSTSTORE_PASSWORD_CONFIG, "THE_TRUSTSTORE_PASS"));
        assertThat(producerProps, hasEntry(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"));
        assertThat(producerProps, hasEntry(MAX_REQUEST_SIZE_CONFIG, "1048576"));
        assertThat(producerProps, hasEntry(LINGER_MS_CONFIG, "0"));
        assertThat(producerProps, hasEntry(SCHEMA_REGISTRY_CONFIG, "http://sr.awseuc1.tst.edh.cnb:8081"));
        assertThat(producerProps, aMapWithSize(16));
    }

    private static void assertConsumerBase(Configs configs) {
        assertThat(configs.consumerBaseGuarantee, equalTo(AT_LEAST_ONCE.name().toLowerCase()));
        assertThat(configs.consumerBasePollTimeoutMs, equalTo(50000L));
    }

    private static void assertDb(Configs configs) {
        assertThat(configs.dbType, equalTo("Oracle"));
        assertThat(configs.dbTable, equalTo("V_BCK_MD_RL_PL_ACTVE_SUBSTANCE"));
        assertThat(configs.dbName, equalTo("APS11R2B.LEV.DE.BAYER.CNB"));
        assertThat(configs.dbSchema, equalTo("THE_DB_SCHEMA"));
        assertThat(configs.dbHost, equalTo("by0q9s.de.bayer.cnb"));
        assertThat(configs.dbPort, equalTo("1522"));
        assertThat(configs.dbUser, equalTo("THE_DB_USER"));
        assertThat(configs.dbPassword, equalTo("THE_DB_PASS"));
        assertThat(configs.dbUpsert, equalTo(true));
        assertThat(configs.dbPrimaryKey, equalTo("PK"));
        assertThat(configs.dbFetch, equalTo(500));
        assertThat(configs.dbServiceName, equalTo("data"));
    }

    private static void assertConsumerBigQuery(Configs configs) {
        assertThat(configs.bigQueryDataset, equalTo("the-dataset"));
        assertThat(configs.bigQueryTable, equalTo("the-table"));
        assertThat(configs.gcsBucketName, equalTo("the-bucket"));
        assertThat(configs.gcsFolderName, equalTo("the-folder"));
        assertThat(configs.googleCredentials, equalTo("/path/to/key.json"));
        assertThat(configs.gcsFilePrefix, equalTo("prefix"));
        assertThat(configs.googleProjectId, equalTo("project-id"));
    }

    private static void assertStatistics(Configs configs) {
        assertThat(configs.statisticsIntermediatePeriodSec, equalTo(60));
    }

    private static void assertNotification(Configs configs) {
        assertThat(configs.notificationKafkaAllowedStatisticsTypesByComma, equalTo(Statistics.Type.INTERMEDIATE.name() + ",FINAL"));
        assertThat(configs.notificationKafkaTopic, equalTo("kafka-notification-topic"));
        assertThat(configs.notificationKafkaBootstrapServers, equalTo("kafka-server:9093"));
        assertThat(configs.notificationKafkaTruststoreLocation, equalTo("c:/dir/truststore"));
        assertThat(configs.notificationKafkaTruststorePassword, equalTo("tspass"));
        assertThat(configs.notificationKafkaKeystoreLocation, equalTo("c:/dir/keystore"));
        assertThat(configs.notificationKafkaKeystorePassword, equalTo("kspass"));
        assertThat(configs.notificationKafkaKeyPassword, equalTo("keypass"));
    }

    private static void assertProducerDbPlain(Configs configs) {
        assertThat(configs.producerDbPlainRangeEnable, equalTo(false));
        assertThat(configs.producerDbPlainRangeColumn, equalTo("date"));
        assertThat(configs.producerDbPlainRangeMin, equalTo("'11 Feb 2020'"));
        assertThat(configs.producerDbPlainRangeMax, equalTo("'12 Feb 2020'"));
        assertThat(configs.producerDbPlainQuery, equalTo("SELECT * FROM MYEVG_PLAIN.V_BCK_MD_RL_PL_ACTVE_SUBSTANCE"));
    }

    private static void assertConsumerS3(Configs configs) {
        assertThat(configs.consumerS3OutputDir, equalTo("c:/Temp"));
        assertThat(configs.consumerS3UploadEmptyFiles, equalTo(false));
        assertThat(configs.consumerS3FormatType, equalTo(AVRO));
        assertThat(configs.consumerS3FormatCsvValueDelimiter, equalTo(';'));
        assertThat(configs.consumerS3FormatCsvRecordDelimiter, equalTo("CRLF"));
        assertThat(configs.consumerS3FormatCsvCharset, equalTo("Windows-1251"));
        assertThat(configs.consumerS3FormatCsvS3KeyName, equalTo("the_folder/the_file.csv"));
        assertThat(configs.consumerS3FormatAvroS3KeyName, equalTo("the_folder/the_file.avro"));
        assertThat(configs.consumerS3FormatBinaryHeaderName, equalTo("filename1"));
        assertThat(configs.consumerS3AwsRegion, equalTo("eu-central-2"));
        assertThat(configs.consumerS3AwsServiceEndpoint, equalTo("https://s3.eu-central-1.amazonaws.com"));
        assertThat(configs.consumerS3AwsBucketName, equalTo("s3-consumer"));
        assertThat(configs.consumerS3AwsAuthType, equalTo(S3AuthType.BASIC.name()));
        assertThat(configs.consumerS3AwsAuthBasicAccessKey, equalTo("the-access-key"));
        assertThat(configs.consumerS3AwsAuthBasicSecretKey, equalTo("the-secret-key"));
        assertThat(configs.consumerS3AwsAuthRoleArn, equalTo("the-arn"));
        assertThat(configs.consumerS3AwsAuthRoleSession, equalTo("the-session"));
        assertThat(configs.consumerS3RetryMaxNumber, equalTo(6));
        assertThat(configs.consumerS3RetryWaitTimeMs, equalTo(7000));
    }

    private static void assertConsumerFile(Configs configs) {
        assertThat(configs.consumerFileFilePath, equalTo("c:/Temp/FileWriterTest/out.csv"));
        assertThat(configs.consumerFileRetryMaxNumber, equalTo(5));
        assertThat(configs.consumerFileRetryWaitTimeMs, equalTo(5000));
        assertThat(configs.consumerFileFileFormat, equalTo(AVRO));
        assertThat(configs.consumerFileFormatCsvValueDelimiter, equalTo(':'));
        assertThat(configs.consumerFileFormatCsvRecordDelimiter, equalTo("CRLF"));
        assertThat(configs.consumerFileFormatCsvCharset, equalTo("Windows-1251"));
        assertThat(configs.consumerFileBinaryOutputDir, equalTo("c:/Temp/binary"));
        assertThat(configs.consumerFileBinaryHeaderName, equalTo("filename"));
    }

    private static void assertProducerSharedoc(Configs configs) {
        assertThat(configs.producerSharedocBaseUrl, equalTo("https://sharedoc.com/api"));
        assertThat(configs.producerSharedocRepository, equalTo("test-repo"));
        assertThat(configs.producerSharedocQuery, equalTo("select * from sd_document"));
        assertThat(configs.producerSharedocUsername, equalTo("the-sharedoc-username"));
        assertThat(configs.producerSharedocPassword, equalTo("the-sharedoc-password"));
        assertThat(configs.producerSharedocMode, equalTo("metadata"));
        assertThat(configs.producerSharedocHttpTimeoutSeconds, equalTo(1000));
        assertThat(configs.producerSharedocChunkSizeBytes, equalTo(1000000));
    }

    private static void assertProducerDbDelta(Configs configs) {
        assertThat(configs.producerDbDeltaType, equalTo(DeltaType.TIMESTAMP.name()));
        assertThat(configs.producerDbDeltaColumn, equalTo("CREATED"));
        assertThat(configs.producerDbDeltaMinOverride, equalTo("'2019-01-01 00:00:00'"));
        assertThat(configs.producerDbDeltaMaxOverride, equalTo("'2020-01-01 00:00:00'"));
        assertThat(configs.producerDbDeltaPollInterval, equalTo(3600));
        assertThat(configs.producerDbDeltaSelectInterval, equalTo("1d"));
        assertThat(configs.producerDbDeltaQuery, equalTo("SELECT * FROM THE_SCHEMA.THE_TABLE"));
    }

    private static void assertProducerCsv(Configs configs) {
        assertThat(configs.producerCsvFilePath, equalTo("c:/path/to/file/csv"));
        assertThat(configs.producerCsvValueDelimiter, equalTo(':'));
        assertThat(configs.producerCsvRecordDelimiter, equalTo("LF"));
        assertThat(configs.producerCsvFileBatchSize, equalTo(10000));
        assertThat(configs.producerCsvCharset, equalTo("Windows-1251"));
        assertThat(configs.producerCsvHeader, equalTo("header1"));
    }

    @BeforeEach
    void invalidate() {
        ConfigFactory.invalidateCaches();
    }

    @Test
    void defaults() throws IOException {
        var emptyFile = Files.createTempFile(ConfigsTest.class.getSimpleName(), ".properties").toFile();
        var configsList = ConfigFileLoader.readConfigsListFromFile(emptyFile);
        assertThat(configsList, hasSize(1));
        var configs = configsList.get(0);
        assertThat(configs.clientType, emptyString());
        assertThat(configs.kafkaCommonTopic, emptyString());
        assertThat(configs.kafkaCommonSchemaRegistryUrl, emptyString());
        assertThat(configs.consumerBaseGuarantee, equalTo(EXACTLY_ONCE.name()));
        assertThat(configs.producerDbPlainMetadataRecordKey, emptyString());

        assertThat(configs.producerCsvFilePath, emptyString());
        assertThat(configs.producerCsvValueDelimiter, equalTo(CSV_VALUE_DELIMITER_DEFAULT));
        assertThat(configs.producerCsvRecordDelimiter, equalTo(CSV_RECORD_DELIMITER_DEFAULT));
        assertThat(configs.producerCsvCharset, equalTo(CSV_CHARSET_DEFAULT));
        assertThat(configs.producerCsvFileBatchSize, equalTo(5000));

        assertThat(configs.producerDbDeltaType, equalTo(DeltaType.TIMESTAMP.name()));
        assertThat(configs.producerDbDeltaColumn, emptyString());
        assertThat(configs.producerDbDeltaMaxOverride, emptyString());
        assertThat(configs.producerDbDeltaMinOverride, emptyString());
        assertThat(configs.producerDbDeltaQuery, emptyString());
        assertThat(configs.producerDbDeltaPollInterval, nullValue());
        assertThat(configs.producerDbDeltaSelectInterval, emptyString());

        assertThat(configs.consumerBasePollTimeoutMs, equalTo(CONSUMER_BASE_POLL_TIMEOUT_MS_DEFAULT));
        assertThat(configs.kafkaConsumerOffset, equalTo(-1L));
        assertThat(configs.kafkaConsumerPartition, equalTo(-1));

        assertThat(configs.dbType, equalTo(DB_TYPE_DEFAULT));
        assertThat(configs.dbTable, emptyString());
        assertThat(configs.dbName, emptyString());
        assertThat(configs.dbSchema, emptyString());
        assertThat(configs.dbHost, emptyString());
        assertThat(configs.dbPort, emptyString());
        assertThat(configs.dbUser, emptyString());
        assertThat(configs.dbPassword, emptyString());
        assertThat(configs.dbUpsert, equalTo(false));
        assertThat(configs.dbPrimaryKey, emptyString());
        assertThat(configs.dbFetch, equalTo(0));
        assertThat(configs.dbServiceName, emptyString());

        assertThat(configs.kafkaConsumerProperties, allOf(
                aMapWithSize(1),
                hasEntry(SECURITY_PROTOCOL_CONFIG, "SSL")
        ));

        assertThat(configs.kafkaProducerProperties, allOf(
                aMapWithSize(1),
                hasEntry(SECURITY_PROTOCOL_CONFIG, "SSL")
        ));

        assertThat(configs.producerSharedocBaseUrl, emptyString());
        assertThat(configs.producerSharedocRepository, emptyString());
        assertThat(configs.producerSharedocQuery, emptyString());
        assertThat(configs.producerSharedocUsername, emptyString());
        assertThat(configs.producerSharedocPassword, emptyString());
        assertThat(configs.producerSharedocMode, emptyString());
        assertThat(configs.producerSharedocHttpTimeoutSeconds, equalTo(PRODUCER_SHAREDOC_HTTP_TIMEOUT_SECONDS_DEFAULT));
        assertThat(configs.producerSharedocChunkSizeBytes, equalTo(PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_DEFAULT));

        assertThat(configs.consumerFileFilePath, emptyString());
        assertThat(configs.consumerFileFileFormat, equalTo(CSV));
        assertThat(configs.consumerFileFormatCsvValueDelimiter, equalTo(CSV_VALUE_DELIMITER_DEFAULT));
        assertThat(configs.consumerFileFormatCsvRecordDelimiter, equalTo(CSV_RECORD_DELIMITER_DEFAULT));
        assertThat(configs.consumerFileFormatCsvCharset, equalTo(CSV_CHARSET_DEFAULT));
        assertThat(configs.consumerFileRetryMaxNumber, equalTo(3));
        assertThat(configs.consumerFileRetryWaitTimeMs, equalTo(1000));
        assertThat(configs.consumerFileBinaryOutputDir, emptyString());
        assertThat(configs.consumerFileBinaryHeaderName, equalTo("filename"));

        assertThat(configs.consumerS3OutputDir, emptyString());
        assertThat(configs.consumerS3UploadEmptyFiles, equalTo(true));
        assertThat(configs.consumerS3FormatType, equalTo(CSV));
        assertThat(configs.consumerS3FormatCsvValueDelimiter, equalTo(CSV_VALUE_DELIMITER_DEFAULT));
        assertThat(configs.consumerS3FormatCsvRecordDelimiter, equalTo(CSV_RECORD_DELIMITER_DEFAULT));
        assertThat(configs.consumerS3FormatCsvCharset, equalTo(CSV_CHARSET_DEFAULT));
        assertThat(configs.consumerS3FormatCsvS3KeyName, emptyString());
        assertThat(configs.consumerS3FormatAvroS3KeyName, emptyString());
        assertThat(configs.consumerS3FormatBinaryHeaderName, equalTo("filename"));
        assertThat(configs.consumerS3AwsRegion, emptyString());
        assertThat(configs.consumerS3AwsServiceEndpoint, equalTo("https://s3..amazonaws.com"));
        assertThat(configs.consumerS3AwsBucketName, emptyString());
        assertThat(configs.consumerS3AwsAuthType, equalTo(S3AuthType.AUTO.name()));
        assertThat(configs.consumerS3AwsAuthBasicAccessKey, emptyString());
        assertThat(configs.consumerS3AwsAuthBasicSecretKey, emptyString());
        assertThat(configs.consumerS3AwsAuthRoleArn, emptyString());
        assertThat(configs.consumerS3AwsAuthRoleSession, emptyString());
        assertThat(configs.consumerS3RetryMaxNumber, equalTo(CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY));
        assertThat(configs.consumerS3RetryWaitTimeMs, equalTo(CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY));

        assertThat(configs.consumerGcsOutputDir, emptyString());
        assertThat(configs.consumerGcsUploadEmptyFiles, equalTo(true));
        assertThat(configs.consumerGcsFormatType, equalTo(CSV));
        assertThat(configs.consumerGcsFormatCsvValueDelimiter, equalTo(CSV_VALUE_DELIMITER_DEFAULT));
        assertThat(configs.consumerGcsFormatCsvRecordDelimiter, equalTo(CSV_RECORD_DELIMITER_DEFAULT));
        assertThat(configs.consumerGcsFormatCsvCharset, equalTo(CSV_CHARSET_DEFAULT));
        assertThat(configs.consumerGcsFormatCsvObjectName, emptyString());
        assertThat(configs.consumerGcsFormatAvroObjectName, emptyString());
        assertThat(configs.consumerGcsFormatBinaryHeaderName, equalTo("filename"));
        assertThat(configs.consumerGcsGcsProjectId, emptyString());
        assertThat(configs.consumerGcsGcsBucketName, emptyString());
        assertThat(configs.consumerGcsGcsKeyFile, emptyString());
        assertThat(configs.consumerGcsRetryMaxNumber, equalTo(CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY));
        assertThat(configs.consumerGcsRetryWaitTimeMs, equalTo(CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY));

        assertThat(configs.statisticsIntermediatePeriodSec, equalTo(STATISTICS_INTERMEDIATE_PERIOD_SEC_DEFAULT));

        assertThat(configs.notificationKafkaAllowedStatisticsTypesByComma,
                equalTo(NOTIFICATION_KAFKA_ALLOWED_STATISTICS_TYPES_BY_COMMA_DEFAULT));
        assertThat(configs.notificationKafkaTopic, emptyString());
        assertThat(configs.notificationKafkaBootstrapServers, emptyString());
        assertThat(configs.notificationKafkaTruststoreLocation, emptyString());
        assertThat(configs.notificationKafkaTruststorePassword, emptyString());
        assertThat(configs.notificationKafkaKeystoreLocation, emptyString());
        assertThat(configs.notificationKafkaKeystorePassword, emptyString());

        //EsConsumer
        assertThat(configs.consumerEsIndexName, emptyString());
        assertThat(configs.consumerEsConnectionAddress, emptyString());
        assertThat(configs.consumerEsConnectionUsername, emptyString());
        assertThat(configs.consumerEsConnectionPassword, emptyString());
        assertThat(configs.consumerEsGuarantee, equalTo(CONSUMER_BASE_GUARANTEE_DEFAULT));
        assertThat(configs.consumerEsPollTimeoutMs, equalTo(CONSUMER_BASE_POLL_TIMEOUT_MS_DEFAULT));
    }

    @Test
    void propertiesFile() {
        var configs = loadConfigFromResource("ConfigsTest_propertiesFile.properties");
        assertClientType(configs, ClientType.PRODUCER_DB_DELTA);
        assertKafka(configs);
        assertDb(configs);
        assertProducerCsv(configs);
        assertProducerDbPlain(configs);
        assertProducerDbDelta(configs);
        assertProducerSharedoc(configs);
        assertConsumerBase(configs);
        assertConsumerFile(configs);
        assertConsumerS3(configs);
        assertConsumerBigQuery(configs);
        assertStatistics(configs);
        assertNotification(configs);
    }

    @Test
    void yamlFileProducerSharedoc() {
        var configs = loadConfigFromResource("ConfigsTest_yamlFile_producer_sharedoc.yaml");
        assertClientType(configs, ClientType.PRODUCER_SHAREDOC);
        assertKafka(configs);
        assertProducerSharedoc(configs);
        assertStatistics(configs);
        assertNotification(configs);
    }

    @Test
    void yamlFileConsumerS3() {
        var configs = loadConfigFromResource("ConfigsTest_yamlFile_consumer_s3.yaml");
        assertClientType(configs, ClientType.CONSUMER_S3);
        assertKafka(configs);
        assertConsumerS3(configs);
        assertStatistics(configs);
        assertNotification(configs);
    }

    @Test
    void yamlFileProducerDbPlain() {
        var configs = loadConfigFromResource("ConfigsTest_yamlFile_producer_db_plain.yaml");
        assertClientType(configs, ClientType.PRODUCER_DB_PLAIN);
        assertKafka(configs);
        assertDb(configs);
        assertProducerDbPlain(configs);
        assertStatistics(configs);
        assertNotification(configs);
    }

    @Test
    void yamlFileProducerDbDelta() {
        var configs = loadConfigFromResource("ConfigsTest_yamlFile_producer_db_delta.yaml");
        assertClientType(configs, ClientType.PRODUCER_DB_DELTA);
        assertKafka(configs);
        assertDb(configs);
        assertProducerDbDelta(configs);
        assertStatistics(configs);
        assertNotification(configs);
    }

    /**
     * Bug https://bijira.intranet.cnb/browse/DAAAA-2507
     */
    @Test
    void tabValueDelimiter() {
        var configs = loadConfigFromResource("ConfigsTest_tabValueDelimiter.properties");
        assertThat(configs.consumerS3FormatCsvValueDelimiter, equalTo('\t'));
        assertThat(configs.consumerFileFormatCsvValueDelimiter, equalTo(' '));
    }

    @Test
    void configsListToString() {
        var configs = loadConfigListFromResource("ConfigsTest_propertiesFile.properties");
        var actStr = configs.toString();
        var expStr = resourceToString(ConfigsTest.class, "ConfigsTest_toStringSecure.txt");
        Assertions.assertEquals(expStr, actStr);
    }

}
