package com.bayer.datahub.libs.config;

import com.bayer.datahub.kafkaclientbuilder.ClientErrorStrategy;
import com.bayer.datahub.libs.kafka.consumer.ConsumerGuarantee;
import com.bayer.datahub.libs.kafka.consumer.cloud.s3.S3AuthType;
import com.bayer.datahub.libs.kafka.producer.db.delta.DeltaType;
import com.bayer.datahub.libs.services.common.statistics.Statistics;
import com.bayer.datahub.libs.services.fileio.CsvRecordDelimiter;
import com.bayer.datahub.libs.services.fileio.FileFormat;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

import static com.bayer.datahub.libs.config.ConfigParser.ConfigTypes.BOOLEAN;
import static com.bayer.datahub.libs.config.ConfigParser.ConfigTypes.CHAR;
import static com.bayer.datahub.libs.config.ConfigParser.ConfigTypes.INT;
import static com.bayer.datahub.libs.config.ConfigParser.ConfigTypes.LONG;
import static com.bayer.datahub.libs.config.ConfigParser.ConfigTypes.STRING;
import static com.bayer.datahub.libs.config.ConfigToString.secureToString;
import static com.bayer.datahub.libs.config.PropertyNames.*;
import static com.bayer.datahub.libs.kafka.Deserializers.AVRO_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Deserializers.STRING_DESERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.AVRO_SERIALIZER;
import static com.bayer.datahub.libs.kafka.Serializers.STRING_SERIALIZER;
import static com.typesafe.config.ConfigFactory.empty;
import static com.typesafe.config.ConfigFactory.parseMap;
import static com.typesafe.config.ConfigValueFactory.fromAnyRef;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

public class Configs {
    //Default Values
    public static final String CLIENT_ERROR_STRATEGY_DEFAULT = ClientErrorStrategy.SKIP_ALL_CLIENTS.name();
    public static final String DB_TYPE_DEFAULT = "none";
    public static final char CSV_VALUE_DELIMITER_DEFAULT = ',';
    public static final boolean CONSUMER_S3_UPLOAD_EMPTY_FILES_DEFAULT = true;
    public static final boolean CONSUMER_GCS_UPLOAD_EMPTY_FILES_DEFAULT = true;
    public static final String CSV_RECORD_DELIMITER_DEFAULT = CsvRecordDelimiter.LF.name();
    public static final String CSV_CHARSET_DEFAULT = StandardCharsets.UTF_8.name();
    public static final String FORMAT_BINARY_HEADER_NAME_DEFAULT = "filename";
    public static final boolean RANGE_ENABLE_DEFAULT = false;
    public static final int PRODUCER_CSV_FILE_BATCH_SIZE_DEFAULT = 5000;
    public static final int PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_DEFAULT = 20 * 1024 * 1024;
    public static final int PRODUCER_SHAREDOC_PARALLELISM_DEFAULT = 5;
    public static final int PRODUCER_SHAREDOC_HTTP_TIMEOUT_SECONDS_DEFAULT = 300;
    public static final String CONSUMER_BASE_GUARANTEE_DEFAULT = ConsumerGuarantee.EXACTLY_ONCE.name();
    public static final long CONSUMER_BASE_POLL_TIMEOUT_MS_DEFAULT = 30000L;
    public static final String PRODUCER_DB_PLAIN_METADATA_FORMAT_DATE_DEFAULT = "yyyy-MM-dd";
    public static final String PRODUCER_DB_DELTA_QUERY_DEFAULT = "";
    public static final String PRODUCER_DB_DELTA_TYPE_DEFAULT = DeltaType.TIMESTAMP.name();
    public static final String PRODUCER_DB_DELTA_SELECT_INTERVAL_DEFAULT = "";
    public static final int STATISTICS_INTERMEDIATE_PERIOD_SEC_DEFAULT = 5 * 60;
    public static final String GCS_LOAD_OPTION_DEFAULT = "avro";
    public static final String GCS_FILE_PREFIX_DEFAULT = "temp";
    public static final int CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY = 3;
    public static final int CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY = 1000;
    public static final String NOTIFICATION_KAFKA_ALLOWED_STATISTICS_TYPES_BY_COMMA_DEFAULT =
            String.join(",", Statistics.Type.INTERMEDIATE.name(), Statistics.Type.FINAL.name());
    private static final Config KAFKA_COMMON_DEFAULT = parseMap(ImmutableMap.<String, String>builder()
            .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
            .build());
    public final String clientType;
    public final ClientErrorStrategy clientErrorStrategy;
    //Kafka Common
    public final String kafkaCommonTopic;
    public final String kafkaCommonSchemaRegistryUrl;
    //Kafka Producer
    public final Map<String, Object> kafkaProducerProperties;
    //Kafka Consumer
    public final Map<String, Object> kafkaConsumerProperties;
    public final Integer kafkaConsumerPartition;
    public final Long kafkaConsumerOffset;
    public final boolean kafkaConsumerExitOnFinish;
    public final String partitionFieldName;
    public final String offsetFieldName;
    public final String keyFieldName;
    public final String timestampFieldName;
    public final boolean includeMetadata;
    public final String deserializerProperty;
    //PlainDbProducer
    public final boolean producerDbPlainRangeEnable;
    public final String producerDbPlainRangeColumn;
    public final String producerDbPlainRangeMin;
    public final String producerDbPlainRangeMax;
    public final String producerDbPlainQuery;
    public final String producerDbPlainMetadataTopic;
    public final String producerDbPlainMetadataDate;
    public final String producerDbPlainMetadataFormatDate;
    public final String producerDbPlainMetadataColumnOrder;
    public final String producerDbPlainMetadataRecordKey;
    //DeltaDbProducer
    public final String producerDbDeltaType;
    public final String producerDbDeltaColumn;
    public final String producerDbDeltaMinOverride;
    public final String producerDbDeltaMaxOverride;
    public final Integer producerDbDeltaPollInterval;
    public final String producerDbDeltaQuery;
    public final String producerDbDeltaSelectInterval;
    //Database
    public final String dbType;
    public final String dbTable;
    public final String dbName;
    public final String dbSchema;
    public final String dbHost;
    public final String dbPort;
    public final String dbUser;
    public final String dbPassword;
    public final boolean dbUpsert;
    public final String dbPrimaryKey;
    public final Integer dbFetch;
    public final String dbServiceName;
    //CsvProducer
    public final FileFormat producerCsvFileFormat;
    public final String producerCsvFilePath;
    public final Integer producerCsvFileBatchSize;
    public final char producerCsvValueDelimiter;
    public final String producerCsvRecordDelimiter;
    public final String producerCsvCharset;
    public final String producerCsvHeader;
    //ShareDocProducer
    public final String producerSharedocBaseUrl;
    public final String producerSharedocRepository;
    public final String producerSharedocQuery;
    public final String producerSharedocUsername;
    public final String producerSharedocPassword;
    public final String producerSharedocMode;
    public final Integer producerSharedocHttpTimeoutSeconds;
    public final Integer producerSharedocChunkSizeBytes;
    public final Integer producerSharedocParallelism;
    //RecordConsumer
    public final String consumerBaseGuarantee;
    public final Long consumerBasePollTimeoutMs;
    //FileConsumer
    public final String consumerFileFilePath;
    public final FileFormat consumerFileFileFormat;
    public final char consumerFileFormatCsvValueDelimiter;
    public final String consumerFileFormatCsvRecordDelimiter;
    public final String consumerFileFormatCsvCharset;
    public final int consumerFileRetryMaxNumber;
    public final int consumerFileRetryWaitTimeMs;
    public final String consumerFileBinaryOutputDir;
    public final String consumerFileBinaryHeaderName;
    //S3Consumer
    public final String consumerS3OutputDir;
    public final boolean consumerS3UploadEmptyFiles;
    public final FileFormat consumerS3FormatType;
    public final char consumerS3FormatCsvValueDelimiter;
    public final String consumerS3FormatCsvRecordDelimiter;
    public final String consumerS3FormatCsvCharset;
    public final String consumerS3FormatCsvS3KeyName;
    public final String consumerS3FormatAvroS3KeyName;
    public final String consumerS3FormatBinaryHeaderName;
    public final String consumerS3AwsRegion;
    public final String consumerS3AwsServiceEndpoint;
    public final String consumerS3AwsBucketName;
    public final String consumerS3AwsAuthType;
    public final String consumerS3AwsAuthBasicAccessKey;
    public final String consumerS3AwsAuthBasicSecretKey;
    public final String consumerS3AwsAuthRoleArn;
    public final String consumerS3AwsAuthRoleSession;
    public final int consumerS3RetryMaxNumber;
    public final int consumerS3RetryWaitTimeMs;

    //GcsConsumer
    public final String consumerGcsOutputDir;
    public final boolean consumerGcsUploadEmptyFiles;
    public final FileFormat consumerGcsFormatType;
    public final char consumerGcsFormatCsvValueDelimiter;
    public final String consumerGcsFormatCsvRecordDelimiter;
    public final String consumerGcsFormatCsvCharset;
    public final String consumerGcsFormatCsvObjectName;
    public final String consumerGcsFormatAvroObjectName;
    public final String consumerGcsFormatBinaryHeaderName;
    public final String consumerGcsGcsProjectId;
    public final String consumerGcsGcsBucketName;
    public final String consumerGcsGcsKeyFile;
    public final int consumerGcsRetryMaxNumber;
    public final int consumerGcsRetryWaitTimeMs;
    public final String consumerGcsGcsProxyHost;
    public final int consumerGcsGcsProxyPort;
    public final String consumerGcsGcsProxyUser;
    public final String consumerGcsGcsProxyPassword;
    public final String consumerGcsGuarantee;
    public final Long consumerGcsPollTimeoutMs;

    //SplunkConsumer
    public final String consumerSplunkHecPeers;
    public final String consumerSplunkHecUseSSL;
    public final String consumerSplunkHecAuthToken;
    public final String consumerSplunkHecUseIndexerAck;
    public final String consumerSplunkHecUseRawEndpoint;
    public final String consumerSplunkHecRawLineBreaker;
    public final String consumerSplunkHecKeystoreLocation;
    public final String consumerSplunkHecKeystorePassword;
    public final String consumerSplunkHecVerifySSL;
    public final String consumerSplunkHecOverrideIndex;
    public final String consumerSplunkHecOverrideHost;
    public final String consumerSplunkHecOverrideSource;
    public final String consumerSplunkHecOverrideSourceType;
    //EsConsumer
    public final String consumerEsIndexName;
    public final FileFormat consumerEsFormatType;
    public final String consumerEsConnectionAddress;
    public final String consumerEsConnectionUsername;
    public final String consumerEsConnectionPassword;
    public final String consumerEsConnectionTruststorePath;
    public final String consumerEsConnectionTruststorePassword;
    public final String consumerEsGuarantee;
    public final Long consumerEsPollTimeoutMs;
    //Statistics
    public final int statisticsIntermediatePeriodSec;
    //KafkaNotification
    public final String notificationKafkaAllowedStatisticsTypesByComma;
    public final String notificationKafkaTopic;
    public final String notificationKafkaBootstrapServers;
    public final String notificationKafkaTruststoreLocation;
    public final String notificationKafkaTruststorePassword;
    public final String notificationKafkaKeystoreLocation;
    public final String notificationKafkaKeystorePassword;
    public final String notificationKafkaKeyPassword;
    //BigQuery
    public final String bigQueryDataset;
    public final String bigQueryTable;
    public final String googleProjectId;
    public final String gcsBucketName;
    public final String gcsFolderName;
    public final String googleCredentials;
    public final String gcsLoadOption;
    public final String gcsFilePrefix;
    public final String bqStreamFormat;
    public final String googleServiceAccountEmail;

    private final ConfigParser parser;

    public Configs(Config config) {
        parser = new ConfigParser(config);

        clientType = parser.get(CLIENT_TYPE_PROPERTY);
        clientErrorStrategy = ClientErrorStrategy.parse(parser.get(CLIENT_ERROR_STRATEGY_PROPERTY, CLIENT_ERROR_STRATEGY_DEFAULT));

        //Kafka Common
        kafkaCommonTopic = parser.get(KAFKA_COMMON_TOPIC_PROPERTY);
        kafkaCommonSchemaRegistryUrl = parser.get(KAFKA_COMMON_SCHEMA_REGISTRY_URL_PROPERTY);

        //Kafka Producer
        kafkaProducerProperties = getProducerProps(config);

        //Kafka Consumer
        kafkaConsumerProperties = getConsumerProps(config);
        kafkaConsumerPartition = parser.get(KAFKA_CONSUMER_PARTITION_PROPERTY, INT);
        kafkaConsumerOffset = parser.get(KAFKA_CONSUMER_OFFSET_PROPERTY, LONG);
        kafkaConsumerExitOnFinish = parser.get(KAFKA_CONSUMER_EXIT_ON_FINISH, BOOLEAN, true);
        includeMetadata = parser.get(KAFKA_CONSUMER_INCLUDE_METADATA, BOOLEAN, false);
        offsetFieldName = parser.get(KAFKA_CONSUMER_OFFSET_FIELD_NAME, "kafka_offset");
        partitionFieldName = parser.get(KAFKA_CONSUMER_PARTITION_FIELD_NAME, "kafka_partition");
        keyFieldName = parser.get(KAFKA_CONSUMER_KEY_FIELD_NAME, "kafka_key");
        timestampFieldName = parser.get(KAFKA_CONSUMER_TIMESTAMP_FIELD_NAME, "kafka_created_ts");
        deserializerProperty = parser.get(KAFKA_CONSUMER_VALUE_DESERIALIZER_PROPERTY, "avro");

        //Database
        dbType = parser.get(PropertyNames.DB_TYPE, DB_TYPE_DEFAULT);
        dbTable = parser.get(PropertyNames.DB_TABLE);
        dbName = parser.get(PropertyNames.DB_NAME);
        dbSchema = parser.get(PropertyNames.DB_SCHEMA);
        dbHost = parser.get(PropertyNames.DB_HOST);
        dbPort = parser.get(PropertyNames.DB_PORT);
        dbUser = parser.get(PropertyNames.DB_USER);
        dbPassword = parser.get(PropertyNames.DB_PASSWORD);
        dbUpsert = parser.get(PropertyNames.DB_UPSERT, BOOLEAN);
        dbPrimaryKey = parser.get(PropertyNames.DB_PRIMARY_KEY);
        dbFetch = parser.get(PropertyNames.DB_FETCH, INT, 0);
        dbServiceName = parser.get(PropertyNames.DB_SERVICE_NAME);

        //PlainDbProducer
        producerDbPlainQuery = parser.get(PRODUCER_DB_PLAIN_QUERY_PROPERTY);
        producerDbPlainRangeEnable = parser.get(PRODUCER_DB_PLAIN_RANGE_ENABLE_PROPERTY, BOOLEAN, RANGE_ENABLE_DEFAULT);
        producerDbPlainRangeColumn = parser.get(PRODUCER_DB_PLAIN_RANGE_COLUMN_PROPERTY);
        producerDbPlainRangeMin = parser.get(PRODUCER_DB_PLAIN_RANGE_MIN_PROPERTY);
        producerDbPlainRangeMax = parser.get(PRODUCER_DB_PLAIN_RANGE_MAX_PROPERTY);
        producerDbPlainMetadataTopic = parser.get(PRODUCER_DB_PLAIN_METADATA_TOPIC_PROPERTY);
        producerDbPlainMetadataDate = parser.get(PRODUCER_DB_PLAIN_METADATA_DATE_PROPERTY);
        producerDbPlainMetadataFormatDate = parser.get(PRODUCER_DB_PLAIN_METADATA_FORMAT_DATE_PROPERTY, PRODUCER_DB_PLAIN_METADATA_FORMAT_DATE_DEFAULT);
        producerDbPlainMetadataColumnOrder = parser.get(PRODUCER_DB_PLAIN_METADATA_COLUMN_ORDER_PROPERTY);
        producerDbPlainMetadataRecordKey = parser.get(PRODUCER_DB_PLAIN_METADATA_RECORD_KEY_PROPERTY);

        //DeltaDbProducer
        producerDbDeltaType = parser.get(PRODUCER_DB_DELTA_TYPE_PROPERTY, PRODUCER_DB_DELTA_TYPE_DEFAULT);
        producerDbDeltaQuery = parser.get(PRODUCER_DB_DELTA_QUERY_PROPERTY, PRODUCER_DB_DELTA_QUERY_DEFAULT);
        producerDbDeltaColumn = parser.get(PRODUCER_DB_DELTA_COLUMN_PROPERTY);
        producerDbDeltaMinOverride = parser.get(PRODUCER_DB_DELTA_MIN_OVERRIDE_PROPERTY);
        producerDbDeltaMaxOverride = parser.get(PRODUCER_DB_DELTA_MAX_OVERRIDE_PROPERTY);
        producerDbDeltaPollInterval = parser.get(PRODUCER_DB_DELTA_POLL_INTERVAL_PROPERTY, INT, null);
        producerDbDeltaSelectInterval = parser.get(PRODUCER_DB_DELTA_SELECT_INTERVAL_PROPERTY, PRODUCER_DB_DELTA_SELECT_INTERVAL_DEFAULT);

        //CsvProducer
        producerCsvFileFormat = FileFormat.parse(parser.get(PRODUCER_CSV_FILE_FORMAT_PROPERTY, FileFormat.CSV.name()),
                PRODUCER_CSV_FILE_FORMAT_PROPERTY);
        producerCsvFilePath = parser.get(PRODUCER_CSV_FILE_PATH_PROPERTY);
        producerCsvValueDelimiter = parser.get(PRODUCER_CSV_VALUE_DELIMITER_PROPERTY, CHAR, CSV_VALUE_DELIMITER_DEFAULT);
        producerCsvRecordDelimiter = parser.get(PRODUCER_CSV_RECORD_DELIMITER_PROPERTY, CSV_RECORD_DELIMITER_DEFAULT);
        producerCsvFileBatchSize = parser.get(PRODUCER_CSV_FILE_BATCH_SIZE_PROPERTY, INT, PRODUCER_CSV_FILE_BATCH_SIZE_DEFAULT);
        producerCsvCharset = parser.get(PRODUCER_CSV_CHARSET_PROPERTY, CSV_CHARSET_DEFAULT);
        producerCsvHeader = parser.get(PRODUCER_CSV_HEADER, "");

        //ShareDocProducer
        producerSharedocBaseUrl = parser.get(PRODUCER_SHAREDOC_BASE_URL_PROPERTY);
        producerSharedocRepository = parser.get(PRODUCER_SHAREDOC_REPOSITORY_PROPERTY);
        producerSharedocQuery = parser.get(PRODUCER_SHAREDOC_QUERY_PROPERTY);
        producerSharedocUsername = parser.get(PRODUCER_SHAREDOC_USERNAME_PROPERTY);
        producerSharedocPassword = parser.get(PRODUCER_SHAREDOC_PASSWORD_PROPERTY);
        producerSharedocMode = parser.get(PRODUCER_SHAREDOC_MODE_PROPERTY);
        producerSharedocHttpTimeoutSeconds = parser.get(PRODUCER_SHAREDOC_HTTP_TIMEOUT_SECONDS_PROPERTY, INT, PRODUCER_SHAREDOC_HTTP_TIMEOUT_SECONDS_DEFAULT);
        producerSharedocChunkSizeBytes = parser.get(PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_PROPERTY, INT, PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_DEFAULT);
        producerSharedocParallelism = parser.get(PRODUCER_SHAREDOC_PARALLELISM_PROPERTY, INT, PRODUCER_SHAREDOC_PARALLELISM_DEFAULT);

        //RecordConsumer
        consumerBaseGuarantee = parser.get(CONSUMER_BASE_GUARANTEE_PROPERTY, CONSUMER_BASE_GUARANTEE_DEFAULT);
        consumerBasePollTimeoutMs = parser.get(CONSUMER_BASE_POLL_TIMEOUT_MS_PROPERTY, LONG, CONSUMER_BASE_POLL_TIMEOUT_MS_DEFAULT);

        //SplunkConsumer
        consumerSplunkHecPeers = parser.get(CONSUMER_SPLUNK_HEC_PEERS_PROPERTY);
        consumerSplunkHecUseSSL = parser.get(CONSUMER_SPLUNK_HEC_SSL_PROPERTY);
        consumerSplunkHecAuthToken = parser.get(CONSUMER_SPLUNK_HEC_AUTH_TOKEN_PROPERTY);
        consumerSplunkHecUseIndexerAck = parser.get(CONSUMER_SPLUNK_HEC_INDEXER_ACK_PROPERTY);
        consumerSplunkHecUseRawEndpoint = parser.get(CONSUMER_SPLUNK_HEC_RAW_ENDPOINT_PROPERTY);
        consumerSplunkHecRawLineBreaker = parser.get(CONSUMER_SPLUNK_HEC_RAW_LINEBREAKER_PROPERTY);
        consumerSplunkHecKeystoreLocation = parser.get(CONSUMER_SPLUNK_HEC_KEYSTORE_LOCATION_PROPERTY);
        consumerSplunkHecKeystorePassword = parser.get(CONSUMER_SPLUNK_HEC_KEYSTORE_PASSWORD_PROPERTY);
        consumerSplunkHecVerifySSL = parser.get(CONSUMER_SPLUNK_HEC_VERIFY_SSL_PROPERTY);
        consumerSplunkHecOverrideIndex = parser.get(CONSUMER_SPLUNK_HEC_OVERRIDE_INDEX_PROPERTY);
        consumerSplunkHecOverrideHost = parser.get(CONSUMER_SPLUNK_HEC_OVERRIDE_HOST_PROPERTY);
        consumerSplunkHecOverrideSource = parser.get(CONSUMER_SPLUNK_HEC_OVERRIDE_SOURCE_PROPERTY);
        consumerSplunkHecOverrideSourceType = parser.get(CONSUMER_SPLUNK_HEC_OVERRIDE_SOURCETYPE_PROPERTY);

        consumerEsIndexName = parser.get(CONSUMER_ES_INDEX_NAME_PROPERTY);
        consumerEsFormatType = FileFormat.parse(parser.get(CONSUMER_ES_FORMAT_TYPE_PROPERTY, FileFormat.AVRO.name()), CONSUMER_ES_FORMAT_TYPE_PROPERTY);
        consumerEsConnectionAddress = parser.get(CONSUMER_ES_CONNECTION_ADDRESS_PROPERTY);
        consumerEsConnectionUsername = parser.get(CONSUMER_ES_CONNECTION_USERNAME_PROPERTY);
        consumerEsConnectionPassword = parser.get(CONSUMER_ES_CONNECTION_PASSWORD_PROPERTY);
        consumerEsConnectionTruststorePath = parser.get(CONSUMER_ES_CONNECTION_TRUSTSTORE_PATH_PROPERTY);
        consumerEsConnectionTruststorePassword = parser.get(CONSUMER_ES_CONNECTION_TRUSTSTORE_PASSWORD_PROPERTY);
        consumerEsGuarantee = parser.get(CONSUMER_ES_CONSUMER_GUARANTEE_PROPERTY, CONSUMER_BASE_GUARANTEE_DEFAULT);
        consumerEsPollTimeoutMs = parser.get(CONSUMER_ES_CONSUMER_POLL_TIMEOUT_MS_PROPERTY, LONG, CONSUMER_BASE_POLL_TIMEOUT_MS_DEFAULT);

        //FileConsumer properties
        consumerFileFileFormat = FileFormat.parse(parser.get(CONSUMER_FILE_FILE_FORMAT_PROPERTY, FileFormat.CSV.name()),
                CONSUMER_FILE_FILE_FORMAT_PROPERTY);
        consumerFileFilePath = parser.get(CONSUMER_FILE_FILE_PATH_PROPERTY);
        consumerFileFormatCsvValueDelimiter = parser.get(CONSUMER_FILE_FORMAT_CSV_VALUE_DELIMITER_PROPERTY, CHAR, CSV_VALUE_DELIMITER_DEFAULT);
        consumerFileFormatCsvRecordDelimiter = parser.get(CONSUMER_FILE_FORMAT_CSV_RECORD_DELIMITER_PROPERTY, CSV_RECORD_DELIMITER_DEFAULT);
        consumerFileFormatCsvCharset = parser.get(CONSUMER_FILE_FORMAT_CSV_CHARSET_PROPERTY, CSV_CHARSET_DEFAULT);
        consumerFileRetryMaxNumber = parser.get(PropertyNames.CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY, INT,
                CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY);
        consumerFileRetryWaitTimeMs = parser.get(PropertyNames.CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY, INT,
                CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY);
        consumerFileBinaryOutputDir = parser.get(CONSUMER_FILE_BINARY_OUTPUT_DIR_PROPERTY, STRING);
        consumerFileBinaryHeaderName = parser.get(CONSUMER_FILE_BINARY_HEADER_NAME_PROPERTY, STRING,
                FORMAT_BINARY_HEADER_NAME_DEFAULT);

        //S3Consumer
        consumerS3OutputDir = parser.get(CONSUMER_S3_OUTPUT_DIR_PROPERTY, STRING);
        consumerS3UploadEmptyFiles = parser.get(CONSUMER_S3_UPLOAD_EMPTY_FILES_PROPERTY, BOOLEAN,
                CONSUMER_S3_UPLOAD_EMPTY_FILES_DEFAULT);
        consumerS3FormatType = FileFormat.parse(parser.get(CONSUMER_S3_FORMAT_TYPE_PROPERTY, FileFormat.CSV.name()),
                CONSUMER_S3_FORMAT_TYPE_PROPERTY);
        consumerS3FormatCsvValueDelimiter = parser.get(CONSUMER_S3_FORMAT_CSV_VALUE_DELIMITER_PROPERTY, CHAR,
                CSV_VALUE_DELIMITER_DEFAULT);
        consumerS3FormatCsvRecordDelimiter = parser.get(CONSUMER_S3_FORMAT_CSV_RECORD_DELIMITER_PROPERTY,
                CSV_RECORD_DELIMITER_DEFAULT);
        consumerS3FormatCsvCharset = parser.get(CONSUMER_S3_FORMAT_CSV_CHARSET_PROPERTY, CSV_CHARSET_DEFAULT);
        consumerS3FormatCsvS3KeyName = parser.get(CONSUMER_S3_FORMAT_CSV_S3_KEY_NAME_PROPERTY);
        consumerS3FormatAvroS3KeyName = parser.get(CONSUMER_S3_FORMAT_AVRO_S3_KEY_NAME_PROPERTY);
        consumerS3FormatBinaryHeaderName = parser.get(CONSUMER_S3_FORMAT_BINARY_HEADER_NAME_PROPERTY, STRING,
                FORMAT_BINARY_HEADER_NAME_DEFAULT);
        consumerS3AwsRegion = parser.get(CONSUMER_S3_AWS_REGION_PROPERTY);
        // CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY property is used for unit-test purposes only
        consumerS3AwsServiceEndpoint = parser.get(CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY, "https://s3." + consumerS3AwsRegion + ".amazonaws.com");
        consumerS3AwsBucketName = parser.get(CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY);
        consumerS3AwsAuthType = parser.get(CONSUMER_S3_AWS_AUTH_TYPE_PROPERTY, S3AuthType.AUTO.name());
        consumerS3AwsAuthBasicAccessKey = parser.get(CONSUMER_S3_AWS_AUTH_BASIC_ACCESS_KEY_PROPERTY);
        consumerS3AwsAuthBasicSecretKey = parser.get(CONSUMER_S3_AWS_AUTH_BASIC_SECRET_KEY_PROPERTY);
        consumerS3AwsAuthRoleArn = parser.get(CONSUMER_S3_AWS_AUTH_ROLE_ARN_PROPERTY);
        consumerS3AwsAuthRoleSession = parser.get(CONSUMER_S3_AWS_AUTH_ROLE_SESSION_PROPERTY);
        consumerS3RetryMaxNumber = parser.get(CONSUMER_S3_RETRY_MAX_NUMBER_PROPERTY, INT,
                CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY);
        consumerS3RetryWaitTimeMs = parser.get(CONSUMER_S3_RETRY_WAIT_TIME_MS_PROPERTY, INT,
                CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY);

        //GcsConsumer
        consumerGcsOutputDir = parser.get(CONSUMER_GCS_OUTPUT_DIR_PROPERTY);
        consumerGcsUploadEmptyFiles = parser.get(CONSUMER_GCS_UPLOAD_EMPTY_FILES_PROPERTY, BOOLEAN,
                CONSUMER_GCS_UPLOAD_EMPTY_FILES_DEFAULT);
        consumerGcsFormatType = FileFormat.parse(parser.get(CONSUMER_GCS_FORMAT_TYPE_PROPERTY, FileFormat.CSV.name()),
                CONSUMER_GCS_FORMAT_TYPE_PROPERTY);
        consumerGcsFormatCsvValueDelimiter = parser.get(CONSUMER_GCS_FORMAT_CSV_VALUE_DELIMITER_PROPERTY, CHAR,
                CSV_VALUE_DELIMITER_DEFAULT);
        consumerGcsFormatCsvRecordDelimiter = parser.get(CONSUMER_GCS_FORMAT_CSV_RECORD_DELIMITER_PROPERTY,
                CSV_RECORD_DELIMITER_DEFAULT);
        consumerGcsFormatCsvCharset = parser.get(CONSUMER_GCS_FORMAT_CSV_CHARSET_PROPERTY, CSV_CHARSET_DEFAULT);
        consumerGcsFormatCsvObjectName = parser.get(CONSUMER_GCS_FORMAT_CSV_OBJECT_NAME_PROPERTY);
        consumerGcsFormatAvroObjectName = parser.get(CONSUMER_GCS_FORMAT_AVRO_OBJECT_NAME_PROPERTY);
        consumerGcsFormatBinaryHeaderName = parser.get(CONSUMER_GCS_FORMAT_BINARY_HEADER_NAME_PROPERTY,
                FORMAT_BINARY_HEADER_NAME_DEFAULT);
        consumerGcsGcsProjectId = parser.get(CONSUMER_GCS_GCS_PROJECT_ID_PROPERTY);
        consumerGcsGcsBucketName = parser.get(CONSUMER_GCS_GCS_BUCKET_NAME_PROPERTY);
        consumerGcsGcsKeyFile = parser.get(CONSUMER_GCS_GCS_KEY_FILE_PROPERTY);
        consumerGcsRetryMaxNumber = parser.get(CONSUMER_GCS_RETRY_MAX_NUMBER_PROPERTY, INT, CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY);
        consumerGcsRetryWaitTimeMs = parser.get(CONSUMER_GCS_RETRY_WAIT_TIME_MS_PROPERTY, INT, CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY);
        consumerGcsGcsProxyHost = parser.get(CONSUMER_GCS_GCS_PROXY_HOST_PROPERTY);
        consumerGcsGcsProxyPort = parser.get(CONSUMER_GCS_GCS_PROXY_PORT_PROPERTY, INT);
        consumerGcsGcsProxyUser = parser.get(CONSUMER_GCS_GCS_PROXY_USER_PROPERTY);
        consumerGcsGcsProxyPassword = parser.get(CONSUMER_GCS_GCS_PROXY_PASSWORD_PROPERTY);
        consumerGcsGuarantee = parser.get(CONSUMER_GCS_GUARANTEE_PROPERTY, CONSUMER_BASE_GUARANTEE_DEFAULT);
        consumerGcsPollTimeoutMs = parser.get(CONSUMER_GCS_POLL_TIMEOUT_MS_PROPERTY, LONG, CONSUMER_BASE_POLL_TIMEOUT_MS_DEFAULT);

        //bigquery
        bigQueryDataset = parser.get(BQ_DATASET_NAME);
        bigQueryTable = parser.get(BQ_TABLE_NAME);
        googleProjectId = parser.get(GOOGLE_PROJECT_ID);
        gcsBucketName = parser.get(GCS_BUCKET_NAME);
        gcsFolderName = parser.get(GCS_FOLDER_NAME);
        googleCredentials = parser.get(GOOGLE_APPLICATION_CREDS);
        gcsLoadOption = parser.get(GCS_LOAD_OPTION, GCS_LOAD_OPTION_DEFAULT);
        gcsFilePrefix = parser.get(GCS_FILE_PREFIX, GCS_FILE_PREFIX_DEFAULT);
        bqStreamFormat = parser.get(BQ_STREAM_FORMAT, "avro");
        googleServiceAccountEmail = parser.get(GCP_SVC_ACCT_EMAIL);

        //Statistics
        statisticsIntermediatePeriodSec = parser.get(STATISTICS_INTERMEDIATE_PERIOD_SEC_PROPERTY, INT,
                STATISTICS_INTERMEDIATE_PERIOD_SEC_DEFAULT);

        //Notification to Kafka
        notificationKafkaAllowedStatisticsTypesByComma =
                parser.get(NOTIFICATION_KAFKA_ALLOWED_STATISTICS_TYPES_BY_COMMA_PROPERTY,
                        NOTIFICATION_KAFKA_ALLOWED_STATISTICS_TYPES_BY_COMMA_DEFAULT);
        notificationKafkaTopic = parser.get(NOTIFICATION_KAFKA_TOPIC_PROPERTY);
        notificationKafkaBootstrapServers = parser.get(NOTIFICATION_KAFKA_BOOTSTRAP_SERVERS_PROPERTY);
        notificationKafkaTruststoreLocation = parser.get(NOTIFICATION_KAFKA_TRUSTSTORE_LOCATION_PROPERTY);
        notificationKafkaTruststorePassword = parser.get(NOTIFICATION_KAFKA_TRUSTSTORE_PASSWORD_PROPERTY);
        notificationKafkaKeystoreLocation = parser.get(NOTIFICATION_KAFKA_KEYSTORE_LOCATION_PROPERTY);
        notificationKafkaKeystorePassword = parser.get(NOTIFICATION_KAFKA_KEYSTORE_PASSWORD_PROPERTY);
        notificationKafkaKeyPassword = parser.get(NOTIFICATION_KAFKA_KEY_PASSWORD_PROPERTY);
    }

    public Configs() {
        this(ConfigFactory.load(empty()));
    }

    public Configs(Map<String, ?> propertyMap) {
        this(ConfigFactory.parseMap(propertyMap));
    }

    private static Map<String, Object> configToProperties(Config config) {
        return config.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().unwrapped().toString()));
    }

    private static Config getKafkaCommonProps(Config config) {
        return config.hasPath(KAFKA_COMMON_PATH) ?
                config
                        .withoutPath(KAFKA_COMMON_TOPIC_PROPERTY)
                        .getConfig(KAFKA_COMMON_PATH)
                        .withFallback(KAFKA_COMMON_DEFAULT)
                : KAFKA_COMMON_DEFAULT;
    }

    private Map<String, Object> getProducerProps(Config config) {
        var kafkaCommon = getKafkaCommonProps(config);
        var kafkaProducer = config.hasPath(KAFKA_PRODUCER_PATH) ?
                config.getConfig(KAFKA_PRODUCER_PATH).withFallback(kafkaCommon) : kafkaCommon;

        var keySerializer = getSerializerClass(KAFKA_PRODUCER_KEY_SERIALIZER_PROPERTY);
        if (isNotBlank(keySerializer)) {
            kafkaProducer = kafkaProducer.withValue(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, fromAnyRef(keySerializer));
        }

        var valueSerializer = getSerializerClass(KAFKA_PRODUCER_VALUE_SERIALIZER_PROPERTY);
        if (isNotBlank(valueSerializer)) {
            kafkaProducer = kafkaProducer.withValue(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, fromAnyRef(valueSerializer));
        }
        return configToProperties(kafkaProducer);
    }

    private Map<String, Object> getConsumerProps(Config config) {
        var kafkaCommon = getKafkaCommonProps(config);
        var kafkaConsumer = config.hasPath(KAFKA_CONSUMER_PATH) ?
                config.getConfig(KAFKA_CONSUMER_PATH).withFallback(kafkaCommon) : kafkaCommon;

        var keyDeserializer = getDeserializerClass(KAFKA_CONSUMER_KEY_DESERIALIZER_PROPERTY);
        if (isNotBlank(keyDeserializer)) {
            kafkaConsumer = kafkaConsumer.withValue(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, fromAnyRef(keyDeserializer));
        }

        var valueDeserializer = getDeserializerClass(KAFKA_CONSUMER_VALUE_DESERIALIZER_PROPERTY);
        if (isNotBlank(valueDeserializer)) {
            kafkaConsumer = kafkaConsumer.withValue(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, fromAnyRef(valueDeserializer));
        }

        return configToProperties(kafkaConsumer);
    }

    private String getSerializerClass(String propertyName) {
        String value = parser.get(propertyName);
        if (value == null) {
            return null;
        }
        var lowerCaseValue = value.toLowerCase();
        String deserializerClass;
        switch (lowerCaseValue) {
            case "avro":
                deserializerClass = AVRO_SERIALIZER;
                break;
            case "string":
                deserializerClass = STRING_SERIALIZER;
                break;
            default:
                deserializerClass = value;
        }
        return deserializerClass;
    }

    private String getDeserializerClass(String propertyName) {
        String value = parser.get(propertyName);
        if (value == null) {
            return null;
        }
        var lowerCaseValue = value.toLowerCase();
        String deserializerClass;
        switch (lowerCaseValue) {
            case "avro":
                deserializerClass = AVRO_DESERIALIZER;
                break;
            case "string":
                deserializerClass = STRING_DESERIALIZER;
                break;
            default:
                deserializerClass = value;
        }
        return deserializerClass;
    }

    public String toStringWithProperties() {
        return secureToString("Base configs (excluding producer and consumer):", parser.getAllProperties());
    }
}