package com.bayer.datahub.libs.config;

public interface PropertyNames {
    String CLIENT_TYPE_PROPERTY = "client.type";
    String CLIENT_ERROR_STRATEGY_PROPERTY = "client.error.strategy";

    //Kafka Common
    String KAFKA_COMMON_PATH = "kafka.common";
    String KAFKA_COMMON_TOPIC_PROPERTY = "kafka.common.topic";
    String KAFKA_COMMON_SCHEMA_REGISTRY_URL_PROPERTY = "kafka.common.schema.registry.url";
    String KAFKA_COMMON_SSL_KEY_PASSWORD_PROPERTY = "kafka.common.ssl.key.password";
    String KAFKA_COMMON_SSL_TRUSTSTORE_PASSWORD_PROPERTY = "kafka.common.ssl.truststore.password";
    String KAFKA_COMMON_SSL_KEYSTORE_PASSWORD_PROPERTY = "kafka.common.ssl.keystore.password";

    //Kafka Producer
    String KAFKA_PRODUCER_PATH = "kafka.producer";
    String KAFKA_PRODUCER_KEY_SERIALIZER_PROPERTY = "kafka.producer.key.serializer";
    String KAFKA_PRODUCER_VALUE_SERIALIZER_PROPERTY = "kafka.producer.value.serializer";

    //Kafka Consumer
    String KAFKA_CONSUMER_PATH = "kafka.consumer";
    String KAFKA_CONSUMER_KEY_DESERIALIZER_PROPERTY = "kafka.consumer.key.deserializer";
    String KAFKA_CONSUMER_VALUE_DESERIALIZER_PROPERTY = "kafka.consumer.value.deserializer";
    String KAFKA_CONSUMER_PARTITION_PROPERTY = "kafka.consumer.partition";
    String KAFKA_CONSUMER_OFFSET_PROPERTY = "kafka.consumer.offset";
    String KAFKA_CONSUMER_EXIT_ON_FINISH = "kafka.consumer.exit.on.finish";
    String KAFKA_CONSUMER_INCLUDE_METADATA = "kafka.consumer.include.metadata";
    String KAFKA_CONSUMER_PARTITION_FIELD_NAME = "kafka.consumer.partition.field.name";
    String KAFKA_CONSUMER_OFFSET_FIELD_NAME = "kafka.consumer.offset.field.name";
    String KAFKA_CONSUMER_KEY_FIELD_NAME = "kafka.consumer.key.field.name";
    String KAFKA_CONSUMER_TIMESTAMP_FIELD_NAME = "kafka.consumer.timestamp.field.name";

    //CsvProducer
    String PRODUCER_CSV_FILE_FORMAT_PROPERTY = "producer.csv.file.format";
    String PRODUCER_CSV_FILE_PATH_PROPERTY = "producer.csv.file.path";
    String PRODUCER_CSV_VALUE_DELIMITER_PROPERTY = "producer.csv.value.delimiter";
    String PRODUCER_CSV_RECORD_DELIMITER_PROPERTY = "producer.csv.record.delimiter";
    String PRODUCER_CSV_FILE_BATCH_SIZE_PROPERTY = "producer.csv.file.batch.size";
    String PRODUCER_CSV_CHARSET_PROPERTY = "producer.csv.charset";
    String PRODUCER_CSV_HEADER = "producer.csv.header";

    //ShareDocProducer
    String PRODUCER_SHAREDOC_BASE_URL_PROPERTY = "producer.sharedoc.base.url";
    String PRODUCER_SHAREDOC_REPOSITORY_PROPERTY = "producer.sharedoc.repository";
    String PRODUCER_SHAREDOC_QUERY_PROPERTY = "producer.sharedoc.query";
    String PRODUCER_SHAREDOC_USERNAME_PROPERTY = "producer.sharedoc.username";
    String PRODUCER_SHAREDOC_PASSWORD_PROPERTY = "producer.sharedoc.password";
    String PRODUCER_SHAREDOC_MODE_PROPERTY = "producer.sharedoc.mode";
    String PRODUCER_SHAREDOC_HTTP_TIMEOUT_SECONDS_PROPERTY = "producer.sharedoc.http.timeout.seconds";
    String PRODUCER_SHAREDOC_CHUNK_SIZE_BYTES_PROPERTY = "producer.sharedoc.chunk.size.bytes";
    String PRODUCER_SHAREDOC_PARALLELISM_PROPERTY = "producer.sharedoc.parallelism";

    //PlainDbProducer
    String PRODUCER_DB_PLAIN_RANGE_ENABLE_PROPERTY = "producer.db.plain.range.enable";
    String PRODUCER_DB_PLAIN_RANGE_COLUMN_PROPERTY = "producer.db.plain.range.column";
    String PRODUCER_DB_PLAIN_RANGE_MIN_PROPERTY = "producer.db.plain.range.min";
    String PRODUCER_DB_PLAIN_RANGE_MAX_PROPERTY = "producer.db.plain.range.max";
    String PRODUCER_DB_PLAIN_QUERY_PROPERTY = "producer.db.plain.query";
    String PRODUCER_DB_PLAIN_METADATA_TOPIC_PROPERTY = "producer.db.plain.metadata.topic";
    String PRODUCER_DB_PLAIN_METADATA_DATE_PROPERTY = "producer.db.plain.metadata.date";
    String PRODUCER_DB_PLAIN_METADATA_FORMAT_DATE_PROPERTY = "producer.db.plain.metadata.format.date";
    String PRODUCER_DB_PLAIN_METADATA_COLUMN_ORDER_PROPERTY = "producer.db.plain.metadata.column.order";
    String PRODUCER_DB_PLAIN_METADATA_RECORD_KEY_PROPERTY = "producer.db.plain.metadata.record.key";

    //DeltaDbProducer
    String PRODUCER_DB_DELTA_TYPE_PROPERTY = "producer.db.delta.type";
    String PRODUCER_DB_DELTA_QUERY_PROPERTY = "producer.db.delta.query";
    String PRODUCER_DB_DELTA_COLUMN_PROPERTY = "producer.db.delta.column";
    String PRODUCER_DB_DELTA_MIN_OVERRIDE_PROPERTY = "producer.db.delta.min.override";
    String PRODUCER_DB_DELTA_MAX_OVERRIDE_PROPERTY = "producer.db.delta.max.override";
    String PRODUCER_DB_DELTA_POLL_INTERVAL_PROPERTY = "producer.db.delta.poll.interval";
    String PRODUCER_DB_DELTA_SELECT_INTERVAL_PROPERTY = "producer.db.delta.select.interval";

    //RecordConsumer
    String CONSUMER_BASE_GUARANTEE_PROPERTY = "consumer.base.guarantee";
    String CONSUMER_BASE_POLL_TIMEOUT_MS_PROPERTY = "consumer.base.poll.timeout.ms";

    //FileConsumer
    String CONSUMER_FILE_FILE_FORMAT_PROPERTY = "consumer.file.file.format";
    String CONSUMER_FILE_FILE_PATH_PROPERTY = "consumer.file.file.path";
    String CONSUMER_FILE_FORMAT_CSV_VALUE_DELIMITER_PROPERTY = "consumer.file.format.csv.value.delimiter";
    String CONSUMER_FILE_FORMAT_CSV_RECORD_DELIMITER_PROPERTY = "consumer.file.format.csv.record.delimiter";
    String CONSUMER_FILE_FORMAT_CSV_CHARSET_PROPERTY = "consumer.file.format.csv.charset";
    String CONSUMER_FILE_RETRY_MAX_NUMBER_PROPERTY = "consumer.file.retry.max.number";
    String CONSUMER_FILE_RETRY_WAIT_TIME_MS_PROPERTY = "consumer.file.retry.wait.time.ms";
    String CONSUMER_FILE_BINARY_OUTPUT_DIR_PROPERTY = "consumer.file.binary.output.dir";
    String CONSUMER_FILE_BINARY_HEADER_NAME_PROPERTY = "consumer.file.binary.header.name";

    // S3Consumer
    String CONSUMER_S3_OUTPUT_DIR_PROPERTY = "consumer.s3.output.dir";
    String CONSUMER_S3_UPLOAD_EMPTY_FILES_PROPERTY = "consumer.s3.upload.empty.files";
    String CONSUMER_S3_FORMAT_TYPE_PROPERTY = "consumer.s3.format.type";
    String CONSUMER_S3_FORMAT_CSV_VALUE_DELIMITER_PROPERTY = "consumer.s3.format.csv.value.delimiter";
    String CONSUMER_S3_FORMAT_CSV_RECORD_DELIMITER_PROPERTY = "consumer.s3.format.csv.record.delimiter";
    String CONSUMER_S3_FORMAT_CSV_CHARSET_PROPERTY = "consumer.s3.format.csv.charset";
    String CONSUMER_S3_FORMAT_CSV_S3_KEY_NAME_PROPERTY = "consumer.s3.format.csv.s3.key.name";
    String CONSUMER_S3_FORMAT_AVRO_S3_KEY_NAME_PROPERTY = "consumer.s3.format.avro.s3.key.name";
    String CONSUMER_S3_FORMAT_BINARY_HEADER_NAME_PROPERTY = "consumer.s3.format.binary.header.name";
    String CONSUMER_S3_AWS_REGION_PROPERTY = "consumer.s3.aws.region";
    String CONSUMER_S3_AWS_SERVICE_ENDPOINT_PROPERTY = "consumer.s3.aws.service.endpoint";
    String CONSUMER_S3_AWS_BUCKET_NAME_PROPERTY = "consumer.s3.aws.bucket.name";
    String CONSUMER_S3_AWS_AUTH_TYPE_PROPERTY = "consumer.s3.aws.auth.type";
    String CONSUMER_S3_AWS_AUTH_BASIC_ACCESS_KEY_PROPERTY = "consumer.s3.aws.auth.basic.access.key";
    String CONSUMER_S3_AWS_AUTH_BASIC_SECRET_KEY_PROPERTY = "consumer.s3.aws.auth.basic.secret.key";
    String CONSUMER_S3_AWS_AUTH_ROLE_ARN_PROPERTY = "consumer.s3.aws.auth.role.arn";
    String CONSUMER_S3_AWS_AUTH_ROLE_SESSION_PROPERTY = "consumer.s3.aws.auth.role.session";
    String CONSUMER_S3_RETRY_MAX_NUMBER_PROPERTY = "consumer.s3.retry.max.number";
    String CONSUMER_S3_RETRY_WAIT_TIME_MS_PROPERTY = "consumer.s3.retry.wait.time.ms";

    // GcsConsumer
    String CONSUMER_GCS_OUTPUT_DIR_PROPERTY = "consumer.gcs.output.dir";
    String CONSUMER_GCS_UPLOAD_EMPTY_FILES_PROPERTY = "consumer.gcs.upload.empty.files";
    String CONSUMER_GCS_FORMAT_TYPE_PROPERTY = "consumer.gcs.format.type";
    String CONSUMER_GCS_FORMAT_CSV_VALUE_DELIMITER_PROPERTY = "consumer.gcs.format.csv.value.delimiter";
    String CONSUMER_GCS_FORMAT_CSV_RECORD_DELIMITER_PROPERTY = "consumer.gcs.format.csv.record.delimiter";
    String CONSUMER_GCS_FORMAT_CSV_CHARSET_PROPERTY = "consumer.gcs.format.csv.charset";
    String CONSUMER_GCS_FORMAT_CSV_OBJECT_NAME_PROPERTY = "consumer.gcs.format.csv.object.name";
    String CONSUMER_GCS_FORMAT_AVRO_OBJECT_NAME_PROPERTY = "consumer.gcs.format.avro.object.name";
    String CONSUMER_GCS_FORMAT_BINARY_HEADER_NAME_PROPERTY = "consumer.gcs.format.binary.header.name";
    String CONSUMER_GCS_GCS_PROJECT_ID_PROPERTY = "consumer.gcs.gcs.project.id";
    String CONSUMER_GCS_GCS_BUCKET_NAME_PROPERTY = "consumer.gcs.gcs.bucket.name";
    String CONSUMER_GCS_GCS_KEY_FILE_PROPERTY = "consumer.gcs.gcs.key.file";
    String CONSUMER_GCS_GCS_PROXY_HOST_PROPERTY = "consumer.gcs.gcs.proxy.host";
    String CONSUMER_GCS_GCS_PROXY_PORT_PROPERTY = "consumer.gcs.gcs.proxy.port";
    String CONSUMER_GCS_GCS_PROXY_USER_PROPERTY = "consumer.gcs.gcs.proxy.user";
    String CONSUMER_GCS_GCS_PROXY_PASSWORD_PROPERTY = "consumer.gcs.gcs.proxy.password";
    String CONSUMER_GCS_RETRY_MAX_NUMBER_PROPERTY = "consumer.gcs.retry.max.number";
    String CONSUMER_GCS_RETRY_WAIT_TIME_MS_PROPERTY = "consumer.gcs.retry.wait.time.ms";
    String CONSUMER_GCS_GUARANTEE_PROPERTY = "consumer.gcs.guarantee";
    String CONSUMER_GCS_POLL_TIMEOUT_MS_PROPERTY = "consumer.gcs.poll.timeout.ms";

    //SplunkConsumer
    String CONSUMER_SPLUNK_HEC_PEERS_PROPERTY = "consumer.splunk.hec.peers";
    String CONSUMER_SPLUNK_HEC_SSL_PROPERTY = "consumer.splunk.hec.ssl";
    String CONSUMER_SPLUNK_HEC_AUTH_TOKEN_PROPERTY = "consumer.splunk.hec.auth.token";
    String CONSUMER_SPLUNK_HEC_INDEXER_ACK_PROPERTY = "consumer.splunk.hec.indexer.ack";
    String CONSUMER_SPLUNK_HEC_RAW_ENDPOINT_PROPERTY = "consumer.splunk.hec.raw.endpoint";
    String CONSUMER_SPLUNK_HEC_RAW_LINEBREAKER_PROPERTY = "consumer.splunk.hec.raw.lineBreaker";
    String CONSUMER_SPLUNK_HEC_KEYSTORE_LOCATION_PROPERTY = "consumer.splunk.hec.keystore.location";
    String CONSUMER_SPLUNK_HEC_KEYSTORE_PASSWORD_PROPERTY = "consumer.splunk.hec.keystore.password";
    String CONSUMER_SPLUNK_HEC_VERIFY_SSL_PROPERTY = "consumer.splunk.hec.verify.ssl";
    String CONSUMER_SPLUNK_HEC_OVERRIDE_INDEX_PROPERTY = "consumer.splunk.hec.override.index";
    String CONSUMER_SPLUNK_HEC_OVERRIDE_HOST_PROPERTY = "consumer.splunk.hec.override.host";
    String CONSUMER_SPLUNK_HEC_OVERRIDE_SOURCE_PROPERTY = "consumer.splunk.hec.override.source";
    String CONSUMER_SPLUNK_HEC_OVERRIDE_SOURCETYPE_PROPERTY = "consumer.splunk.hec.override.sourceType";

    //EsConsumer
    String CONSUMER_ES_INDEX_NAME_PROPERTY = "consumer.es.index.name";
    String CONSUMER_ES_FORMAT_TYPE_PROPERTY = "consumer.es.format.type";
    String CONSUMER_ES_CONNECTION_ADDRESS_PROPERTY = "consumer.es.connection.address";
    String CONSUMER_ES_CONNECTION_USERNAME_PROPERTY = "consumer.es.connection.username";
    String CONSUMER_ES_CONNECTION_PASSWORD_PROPERTY = "consumer.es.connection.password";
    String CONSUMER_ES_CONNECTION_TRUSTSTORE_PATH_PROPERTY = "consumer.es.connection.truststore.path";
    String CONSUMER_ES_CONNECTION_TRUSTSTORE_PASSWORD_PROPERTY = "consumer.es.connection.truststore.password";
    String CONSUMER_ES_CONSUMER_GUARANTEE_PROPERTY = "consumer.es.consumer.guarantee";
    String CONSUMER_ES_CONSUMER_POLL_TIMEOUT_MS_PROPERTY = "consumer.es.consumer.poll.timeout.ms";

    //BQ Consumer
    String BQ_DATASET_NAME = "google.bigquery.dataset";
    String BQ_TABLE_NAME = "google.bigquery.table";
    String GOOGLE_PROJECT_ID = "google.project.id";
    String GCS_FOLDER_NAME = "google.storage.folder.name";
    String GCS_BUCKET_NAME = "google.storage.bucket.name";
    String GOOGLE_APPLICATION_CREDS = "google.application.credentials";
    String GCS_LOAD_OPTION = "gcs.load.option";
    String GCS_FILE_PREFIX = "gcs.file.prefix";
    String BQ_STREAM_FORMAT = "bq.stream.format";
    String GCP_SVC_ACCT_EMAIL = "google.service.account.email";

    //Statistics
    String STATISTICS_INTERMEDIATE_PERIOD_SEC_PROPERTY = "statistics.intermediate.period.sec";

    //KafkaNotification
    String NOTIFICATION_KAFKA_ALLOWED_STATISTICS_TYPES_BY_COMMA_PROPERTY = "notification.kafka.allowed.statistics.types.by.comma";
    String NOTIFICATION_KAFKA_TOPIC_PROPERTY = "notification.kafka.topic";
    String NOTIFICATION_KAFKA_BOOTSTRAP_SERVERS_PROPERTY = "notification.kafka.bootstrap.servers";
    String NOTIFICATION_KAFKA_TRUSTSTORE_LOCATION_PROPERTY = "notification.kafka.truststore.location";
    String NOTIFICATION_KAFKA_TRUSTSTORE_PASSWORD_PROPERTY = "notification.kafka.truststore.password";
    String NOTIFICATION_KAFKA_KEYSTORE_LOCATION_PROPERTY = "notification.kafka.keystore.location";
    String NOTIFICATION_KAFKA_KEYSTORE_PASSWORD_PROPERTY = "notification.kafka.keystore.password";
    String NOTIFICATION_KAFKA_KEY_PASSWORD_PROPERTY = "notification.kafka.key.password";

    //DB
    /**
     * The host to connect to - this can be an IP address or domain name
     */
    String DB_HOST = "db.host";
    /**
     * The port to connect to
     */
    String DB_PORT = "db.port";
    /**
     * The username to use to connect to the database
     */
    String DB_USER = "db.username";
    /**
     * The password to use for connecting to the database
     */
    String DB_PASSWORD = "db.password";
    /**
     * The name of the database - in some implementations this is a service
     */
    String DB_NAME = "db.name";
    /**
     * Service name to connect to (for Oracle databases)
     */
    String DB_SERVICE_NAME = "db.service.name";
    /**
     * The table that will be associated with this connection.
     */
    String DB_TABLE = "db.table";
    /**
     * The schema to use if there is one
     */
    String DB_SCHEMA = "db.schema";
    /**
     * A string representing the particular database implementation - {@literal i.e.}
     * MySql, Oracle, Teradata, etc.
     */
    String DB_TYPE = "db.type";
    /**
     * A boolean indicating whether or not upserts should be performed when saving
     * records to the table. If this is true then there must also be a primary
     * key
     */
    String DB_UPSERT = "db.upsert";
    /**
     * The primary key column for this table. This only needs to be specified
     * if upserts are being performed.
     */
    String DB_PRIMARY_KEY = "db.primary.key";
    /**
     * How many records should be fetched from the database
     */
    String DB_FETCH = "db.fetch";
}
