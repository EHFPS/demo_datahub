# Configurable Kafka Client

## Table of content

[[_TOC_]]

## Overview
This is a configurable Kafka client that at the moment can only be used for a single process (consumer or producer). 
It works with different sources/sinks including:
1. Local files (AVRO, CSV)
1. Databases (Postgres, Oracle, Hana, Teradata, MSSQL, MySQL)
1. Cloud file storages (AWS S3, Google Cloud Storage, Azure Blob Storage)
1. ElasticSearch
1. Google Big Query
1. ShareDoc
1. Splunk

Configuration is provided in a property file and can be overwritten by Java system properties. 

## Configuring the Client
The application will read configs from a `*.properties` file and use it to build and run the client. 
The full list of configs is as follows:

### General Configuration Options

#### Client Configs
| Config                             | Default            | Description |
|------------------------------------|--------------------|-------------|
| `client.type`                      | ""                 | Signals to the application which client to run. Possible values: `CONSUMER_FILE`, `CONSUMER_S3`, `CONSUMER_DB`, `CONSUMER_SPLUNK`, `CONSUMER_BIGQUERY`, `CONSUMER_GCS`, `CONSUMER_BQ_SLT`, `CONSUMER_ES`, `PRODUCER_SHAREDOC`, `PRODUCER_DB_PLAIN`, `PRODUCER_DB_DELTA`, `PRODUCER_CSV`. |
| `client.error.strategy`            | "SKIP_ALL_CLIENTS" | Defines how to handle errors if several clients are configured: skip failed client and continue other clients (`SKIP_CURRENT_CLIENT`) or skip failed and all other clients (`SKIP_ALL_CLIENTS`). |
| `kafka.common.topic`               | ""                 | The topic for produced or consumed messages. |

#### Kafka Common Configs
Standard Kafka properties which can be applied to both producer and consumer. Additional to "General Configuration Options" section.  
Any properties listed in the [Producer Configs documentation](https://kafka.apache.org/documentation/#producerconfigs)
or [Consumer Configs documentation](https://kafka.apache.org/documentation/#consumerconfigs) can be used
(with `kafka.common.` prefix).   

Example of frequently used properties:

| Config                             | Default            | Description |
|------------------------------------|--------------------|-------------|
| `kafka.common.bootstrap.servers`   | ""                 | The domain and port of the kafka cluster to connect to |
| `kafka.common.schema.registry.url` | ""                 | The url for the schema registry |

When connecting to Kafka over the secure port (9093) you must have a cert
(which can be obtained through the DataHub Portal):

| Config                                 | Description                                                   |
|----------------------------------------|---------------------------------------------------------------|
| `kafka.common.ssl.key.password`        | The password used when generating the cert through the portal |
| `kafka.common.ssl.truststore.password` | Same as above                                                 |
| `kafka.common.ssl.keystore.password`   | Same as above                                                 |
| `kafka.common.ssl.truststore.location` | Path the the truststore.jks file                              |
| `kafka.common.ssl.keystore.location`   | Path to the keystore.jks file                                 |

#### Database Configs
Database config is used by "Plain Database Producer", "Delta Database Producer" and "Database Consumer".

| Config            | Default  | Description |
|-------------------|----------|-------------|
| `db.type`         | ""       | If using a database this setting must be filled out. Possible values are: Postgres, Oracle, MSSQL, MySQL, Teradata, Hana. Note that this setting is not case sensitive. |
| `db.upsert`       | ""       | Controls whether the client will use 'upserts' when storing records in a database. Values are true or false |
| `db.primary.key`  | ""       | The name of the field that represents the primary key for a table. This setting must be filled out if db.upsert is set to true |
| `db.username`     | ""       | The username to use to connect to a database |
| `db.password`     | ""       | The password for this particular user |
| `db.host`         | ""       | The address of the machine that hosts the database |
| `db.port`         | ""       | The port to connect to |
| `db.table`        | ""       | The name of the table to read/write records from |
| `db.name`         | ""       | The name of the database to connect to |
| `db.schema`       | ""       | In most cases this is the same as the database name but some SQL databases break their databases down into schemas. This setting can be filled out when applicable |
| `db.service.name` | ""       | Oracle DB service name. |
| `db.fetch`        | 0 (auto) | Max number of rows retrieved from DB at one time. |

#### GCP Configs
| Config                           | Description|
|----------------------------------|------------|
| `google.project.id`              | Google project ID. |
| `google.bigquery.dataset`        | The name of the dataset in which the target table resides|
| `google.bigquery.table`          | The name of the target table in BigQuery |
| `google.storage.bucket.name`     | Just the name of the bucket to write records to before loading into BigQuery. Does not need to be the full path (i.e. 'gs://....') |
| `google.storage.folder.name`     | The name of the folder in the previously defined bucket to write files into. Make sure the bucket and folder are created before running the consumer |
| `google.application.credentials` | The path to a json service account key to authenticate the application to utilize GCP services. This is required if running the application outside of the GCP environment. |
| `gcs.load.option`                | Signals to the client whether files should be loaded into gcs and what format to expect. Accepted values are 'avro', 'json', or 'ndjson' for loading avro,json, or new line delimited json files or 'none' to utilize the streaming api (only avro files are supported for streaming at the moment) |
| `gcs.file.prefix`                | Name prefix of target GCS object. |
| `bq.stream.format`               | Format of streaming bq inserts. Accepted values are 'avro' or 'json' depending on the format of incoming messages. |
| `google.service.account.email`   | Email attached to service account to be used to run the app | 

#### Statistics Configs
Statistics is collected from producer/consumer periodically (intermediate statistics) and once after finishing (final statistics).

| Config                               | Default    | Description                                                    |
|--------------------------------------|------------|----------------------------------------------------------------|
| `statistics.intermediate.period.sec` | 300 (5min) | Period when intermediate statistics is collected (in seconds). |

#### Notification Configs
Sends producer/consumer statistics to a Kafka topic.

| Config                                                 | Default              | Description                                                         |
|--------------------------------------------------------|----------------------|---------------------------------------------------------------------|
| `notification.kafka.allowed.statistics.types.by.comma` | "INTERMEDIATE,FINAL" | List of statistics types allowed to send (`INTERMEDIATE`, `FINAL`). |
| `notification.kafka.topic`                             | ""                   | Target Kafka topic for statistics.                                  |
| `notification.kafka.bootstrap.servers`                 | ""                   | Kafka bootstrap servers.                                            |
| `notification.kafka.truststore.location`               | ""                   | Kafka truststore file.                                              |
| `notification.kafka.truststore.password`               | ""                   | Password for truststore file.                                       |
| `notification.kafka.keystore.location`                 | ""                   | Kafka keystore file.                                                |
| `notification.kafka.keystore.password`                 | ""                   | Password for keystore file.                                         |
| `notification.kafka.key.password`                      | ""                   | Password for private key.                                           |

### Producer Configs

#### Kafka Producer Configs
Standard Kafka properties specific for producers. Additional to "Kafka Common Configs" section.  
Any properties listed in the [Producer Configs documentation](https://kafka.apache.org/documentation/#producerconfigs)
can be used (with `kafka.producer.` prefix).   

Example of frequently used properties:
 
| Config                               | Default  | Description |
|--------------------------------------|----------|-------------|
| `kafka.producer.acks`                | 1        | Acks controls how the producer handles acknowledgement from the brokers when it sends a message. There are three possible settings: <ul><li><code>all</code> - the producer will wait until messages are written to the primary partition and all of it's replicas. This is the best way to guarantee that a message is received as long as at least one in-sync replica is up</li><li><code>0</code> - the producer will not wait for any acknowledgement from the leader and will consider a request complete as soon as the record is sent. This setting essentially disables retries since the producer will never know if there was a failure. This can and probably will lead to some amount of message loss.</li><li> <code>1</code> - the leader will send acknowledgement after writing but will not wait for in-sync replicas to catch up. If the leader goes down before the replicas finish  writing then the message will be lost. A nice balance between all and 0.</li></ul> In summary use <code>0</code> or <code>1</code> for better producer throughput if some amount of message loss is tolerable. Otherwise use <code>all</code>. |
| `kafka.producer.compression.type`    | none     | The compression type the producer will use when it sends messages. The default is no compression. Valid values for this setting are 'none', 'snappy', 'gzip', 'lz4', 'zstd'. Compression is of full batches of data thus the more records that are batched the more effective the compression. In general it is good practice to apply compression, especially if clients are batching requests. The details of each compression type are beyond the scope of this documentation. |
| `kafka.producer.batch.size`          | 16384    | The producer will batch records when making requests. This setting is a cap on the total size of a batched request in bytes. A larger batch size will reduce the total number of requests but could potentially waste memory as a buffer equal to the batch size is always allocated in anticipation of extra records. Smaller batch sizes will increase the number of requests but may decrease throughput. Note that if a single record meets or exceeds the batch size there will be no attempt made to batch more records. As a result, a batch size of 0 will effectively disable batching entirely. |
| `kafka.common.client.id`             | ""       | A nonempty string that can identify a producer. This allows clients to be easily identified by including a logical application name in server-side request logging. |
| `kafka.producer.linger.ms`           | 0        | The client will wait for the amount of time specified by this setting between making requests. When the kafka.producer.batch.size number of bytes for a request is reached the request will be sent regardless of this setting; otherwise, the client will wait for the amount of time specified here in order to allow more records to be batched. In effect this setting will reduce the number of total requests by introducing an artificial delay under low to moderate loads. |
| `kafka.producer.max.request.size`    | 1048576  | The maximum size of a request in bytes. This setting will limit the number of record batches the producer will send in a single request to avoid sending huge requests. This is also effectively a cap on the maximum record batch size. Note that the server has its own cap on record batch size which may be different from this. |
| `kafka.common.request.timeout.ms`    | 30000    | The configuration controls the maximum amount of time the client will wait for the response of a request. If the response is not received before the timeout elapses the client will resend the request if necessary or fail the request if retries are exhausted |
| `kafka.producer.delivery.timeout.ms` | 120000   | This setting controls the upper bound on the time to report success or failure after a call to send(). In other words it limits the amount of time to wait before sending a request. It also limits the amount of time to receive acknowledgement from the broker, and the amount of time allotted for retriable failures (in the event of some transient error like a network issue). In other words the request will be considered failed after this amount of time even if the number of retries is still less than the retries setting. Note that this setting should be >= the sum of kafka.common.request.timeout.ms and kafka.producer.linger.ms. |
| `kafka.producer.key.serializer`      | "string" | Serializer for Kafka record key: `string`, `avro` or serializer full class name (e.g. `org.apache.kafka.common.serialization.ByteArraySerializer`). |
| `kafka.producer.value.serializer`    | "string" | Serializer for Kafka record value: : `string`, `avro` or serializer full class name (e.g. `org.apache.kafka.common.serialization.ByteArraySerializer`). |

#### Plain Database Producer Configs
PlainDbProducer fetches all rows from a database table and produces them into a Kafka topic.

Additional to "Kafka Producer Configs" section:

| Config                                    | Default      | Description                                                                                                    |
|-------------------------------------------|--------------|----------------------------------------------------------------------------------------------------------------|
| `producer.db.plain.query`                 | ""           | SQL select query overrides auto-generated query.                                                               |
| `producer.db.plain.range.enable`          | "false"      | This configuration enable range mechanism. The producer transfers only records that have value in this range.  |
| `producer.db.plain.range.min`             | ""           | This value is a minimum of range.                                                                              |
| `producer.db.plain.range.max`             | ""           | This value is a maximum of range.                                                                              |
| `producer.db.plain.range.column`          | ""           | The name of the column where the values are stored.                                                            |
| `producer.db.plain.metadata.topic`        | ""           | The topic name where the metadata information is stored.                                                       |
| `producer.db.plain.metadata.date`         | ""           | The column name, which contains the last date of change.                                                       |
| `producer.db.plain.metadata.format.date`  | "yyyy-MM-dd" | The format of the date column.                                                                                 |
| `producer.db.plain.metadata.column.order` | ""           | The sorting order of the columns, always sorted by first the date column, then by the column names given here. |
| `producer.db.plain.metadata.record.key`   | ""           | The Kafka record key for metadata.                                                                             |

#### Delta Database Producer Configs
DeltaDbProducer fetches new rows from a database table and produces them into a Kafka topic.

Additional to "Kafka Producer Configs" section:

| Config                              | Default     | Description|
|-------------------------------------|-------------|------------|
| `producer.db.delta.column`          | ""          | Column used to calculate deltas. |
| `producer.db.delta.poll.interval`   | 60          | How often the producer should query the table for new records in seconds. |
| `producer.db.delta.type`            | "TIMESTAMP" | Data type of the delta column: `DATE` or `TIMESTAMP`. |
| `producer.db.delta.query`           | ""          | SQL select query overrides auto-generated query. |
| `producer.db.delta.min.override`    | ""          | Minimal value of the delta column included to select query. |
| `producer.db.delta.max.override`    | ""          | Maximal value of the delta column included to select query. |
| `producer.db.delta.select.interval` | ""          | Select query which return a lot of rows can be split into several queries by the interval. Interval string format: ["ISO-8601"](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/Duration.html#parse(java.lang.CharSequence)) |

***Note** that for _client_ configs that aren't present in your configuration file, the default will be used. Additionally, make sure to fill out the correct configurations for the behavior that you want for the client. For example if you want a consumer that writes to a database you should fill out the consumer and database configurations.
***Note** this list does not represent _all_ of the possible client configurations. If there is a particular configuration or even maybe some use case you would like covered you can submit an issue here to this repo.

#### CSV Producer Configs
CsvProducer reads content of a CSV file and produces it to a Kafka topic. 

Additional to "Kafka Producer Configs" section:

| Config                          | Default | Description                                                                                    |
|---------------------------------|---------|------------------------------------------------------------------------------------------------|
| `producer.csv.file.format`      | "CSV"   | Output file format ("CSV", "AVRO", "BINARY").                                                  |
| `producer.csv.file.path`        | ""      | Path to output file.                                                                           |
| `producer.csv.value.delimiter`  | ","     | CSV column delimiter (for "CSV" format only).                                                  |
| `producer.csv.record.delimiter` | "LF"    | CSV row delimiter: "LF" for Linux, "CR" for MacOs, "CRLF" for Windows (for "CSV" format only). |
| `producer.csv.file.batch.size`  | 5000    | Max number of lines read from CSV file at once.                                                |
| `producer.csv.charset`          | "UTF-8" | CSV file charset (for "CSV" format only).                                                      |
| `producer.csv.header`           | ""      | Optional custom headers for csv files with none. If this is empty the producer will use the first line of the csv file as the header. |

#### ShareDoc Producer Configs
ShareDocProducer loads documents from a ShareDoc repository to a Kafka topic.

Additional to "Kafka Producer Configs" section:

| Config                                   | Default         | Description                                                                   |
|------------------------------------------|-----------------|-------------------------------------------------------------------------------|
| `producer.sharedoc.base.url`             | ""              | ShareDoc server base URL.                                                     |
| `producer.sharedoc.repository`           | ""              | ShareDoc repository name at ShareDoc server.                                  |
| `producer.sharedoc.query`                | ""              | Query for retrieving data from ShareDoc repository.                           |
| `producer.sharedoc.username`             | ""              | User's login for ShareDoc server.                                             |
| `producer.sharedoc.password`             | ""              | User's password for ShareDoc server.                                          |
| `producer.sharedoc.mode`                 | ""              | Transfer document metadata only (`METADATA`) or document content (`CONTENT`). |
| `producer.sharedoc.http.timeout.seconds` | 300             | HTTP timeout for connection to ShareDoc server (seconds).                     |
| `producer.sharedoc.chunk.size.bytes`     | 20971520 (20Mb) | Big ShareDoc records will be chunked by this size (bytes).                    |
| `producer.sharedoc.parallelism`          | 5               | Number of parallel requests to ShareDoc server.                               |

### Consumer Configs

#### Kafka Consumer Configs
Standard Kafka properties specific for consumers. Additional to "Kafka Common Configs" section.  
Any properties listed in the [Consumer Configs documentation](https://kafka.apache.org/documentation/#consumerconfigs) 
can be used (with `kafka.consumer.` prefix).   

Example of frequently used properties:  

| Config                              | Default        | Description |
|-------------------------------------|----------------|-------------|
| `kafka.consumer.group.id`           | ""             | Can be any string. Used to enlist your consumer into group management. This setting is not technically mandatory but should probably always be filled out. |
| `kafka.consumer.auto.offset.reset`  | "latest"       | Auto offset reset config controls where consumers will start consuming messages in the event that there is no known offset to start from. There are four possible  settings: <ul><li><code>earliest</code> - the consumer will start from the earliest offset (i.e. it will read all of the messages in the topic)</li><li><code>latest</code> - the consumer will start from the latest offset (i.e. it will only read new messages)</li><li><code>none</code> - an exception will be thrown if no known offset is found </li><li>anything else - throws an exception</li></ul> |
| `kafka.consumer.max.poll.interval.ms`  | 300000         | The maximum delay between invocations of poll(). If the amount of time specified by this setting passes before another poll then the consumer is considered failed and a rebalance is triggered to reassign partitions to the remaining consumers in the group. |
| `kafka.consumer.max.poll.records`   | 500            | Maximum number of records returned by a single call to <code>poll</code>. Increasing this setting may increase througput at the cost of increased time between polls. |
| `kafka.consumer.offset`             | none           | Fill this setting out to consume from a particular offset. |
| `kafka.consumer.partition`          | 0              | Can be used to consume messages from a particular partition in the case that a topic has multiple partitions. |
| `kafka.consumer.key.deserializer`   | "string"       | Deserializer for Kafka record key: `string`, `avro` or deserializer full class name (e.g. `org.apache.kafka.common.serialization.ByteArrayDeserializer`). |
| `kafka.consumer.value.deserializer` | "string"       | Deserializer for Kafka record value: : `string`, `avro` or deserializer full class name (e.g. `org.apache.kafka.common.serialization.ByteArrayDeserializer`). |
| `consumer.base.guarantee`           | "EXACTLY_ONCE" | Consumer guarantee ("EXACTLY_ONCE", "AT_LEAST_ONCE"). |
| `consumer.base.poll.timeout.ms`     | 30000          | Time out for consumer poll request in milliseconds. |
| `kafka.consumer.exit.on.finish`      | true           | Controls whether the consumer process stops when there are no longer any records in the topic. If set to false the consumer will continuously poll until manually exited. |
| `kafka.consumer.include.metadata`   | false          | Option to enrich consumed messages with kafka metadata (message offset, partition, timestamp, key) |
| `kafka.consumer.partition.field.name` | kafka_partition            | optional custom field name to store metadata |
| `kafka.consumer.key.field.name` | kafka_key                  | optional custom field name to store metadata |
| `kafka.consumer.timestamp.field.name` | kafka_created_ts         | optional custom field name to store metadata |  
| `kafka.consumer.offset.field.name` | kafka_offset               | optional custom field name to store metadata  |

#### File Consumer Configs
FileConsumer consumes records form a Kafka topic and wright them into a file.

Additional to "Kafka Consumer Configs" section:

| Config                                      | Default    | Description |
|---------------------------------------------|------------|-------------|
| `consumer.file.file.format`                 | "CSV"      | Output file format ("CSV", "AVRO", "BINARY"). |
| `consumer.file.file.path`                   | ""         | Path to output file. |
| `consumer.file.format.csv.value.delimiter`  | ","        | CSV column delimiter (for "CSV" format only). |
| `consumer.file.format.csv.record.delimiter` | "LF"       | CSV row delimiter: "LF" for Linux, "CR" for MacOs, "CRLF" for Windows (for "CSV" format only). |
| `consumer.file.format.csv.charset`          | "UTF-8"    | CSV file charset (for "CSV" format only). |
| `consumer.file.retry.max.number`            | 3          | Max retry number for committing offset. |
| `consumer.file.retry.wait.time.ms`          | 1000       | Retry interval for committing offset.|
| `consumer.file.binary.output.dir`           | ""         | Output directory (for "BINARY" format only). |
| `consumer.file.binary.header.name`          | "filename" | Name of Kafka message header where name of file is stored (for "BINARY" format only). |

#### AWS S3 Consumer Configs
S3Consumer consumes records from a Kafka topic to a local file and upload it to a S3 bucket.

Additional to "Kafka Consumer Configs" section:

| Config                                    | Default    | Description |
|-------------------------------------------|------------|-------------|
| `consumer.s3.output.dir`                  | ""         | Local temporary directory for consumed data. |
| `consumer.s3.format.type`                 | "CSV"      | Output file format ("CSV", "AVRO", "BINARY"). |
| `consumer.s3.format.csv.value.delimiter`  | ","        | CSV column delimiter (for "CSV" format only). |
| `consumer.s3.format.csv.record.delimiter` | "LF"       | CSV row delimiter: "LF" for Linux, "CR" for MacOs, "CRLF" for Windows (for "CSV" format only). |
| `consumer.s3.format.csv.s3.key.name`      | ""         | Key name of target S3 object (for "CSV" format only). |
| `consumer.s3.format.avro.s3.key.name`     | ""         | Key name of target S3 object (for "AVRO" format only). |
| `consumer.s3.format.csv.charset`          | "UTF-8"    | CSV file charset (for "CSV" format only). |
| `consumer.s3.format.binary.header.name`   | "filename" | Name of Kafka message header where name of file is stored (for "BINARY" format only). |
| `consumer.s3.upload.empty.files`          | "true"     | Enable or disable uploading empty files. |
| `consumer.s3.aws.region`                  | ""         | AWS region. |
| `consumer.s3.aws.bucket.name`             | ""         | AWS bucket name. |
| `consumer.s3.aws.auth.type`               | "auto"     | AWS authentication type ("auto", "basic", "role"). |
| `consumer.s3.aws.auth.basic.access.key`   | ""         | AWS access key (for "basic" auth type only). |
| `consumer.s3.aws.auth.basic.secret.key`   | ""         | AWS secret key (for "basic" auth type only). |
| `consumer.s3.aws.auth.role.arn`           | ""         | AWS role for assume role (for "role" auth type only). |
| `consumer.s3.aws.auth.role.session`       | ""         | AWS session for assume role (for "role" auth type only).  |
| `consumer.s3.retry.max.number`            | 3          | Max retry number for committing offset. |
| `consumer.s3.retry.wait.time.ms`          | 1000       | Retry interval for committing offset.|

#### Google Cloud Storage Consumer Configs
GcsConsumer consumes records from a Kafka topic to a local file and upload it to a GCS bucket. 

Additional to "Kafka Consumer Configs" section:

| Config                                     | Default        | Description |
|--------------------------------------------|----------------|-------------|
| `consumer.gcs.output.dir`                  | ""             | Local temporary directory for consumed data. |
| `consumer.gcs.guarantee`                   | "EXACTLY_ONCE" | Consumer guarantee ("EXACTLY_ONCE", "AT_LEAST_ONCE"). |
| `consumer.gcs.poll.timeout.ms`             | 30000          | Time out for consumer poll request in milliseconds. |
| `consumer.gcs.format.type`                 | "CSV"          | Output file format ("CSV", "AVRO", "BINARY"). |
| `consumer.gcs.format.csv.value.delimiter`  | ","            | CSV column delimiter (for "CSV" format only). |
| `consumer.gcs.format.csv.record.delimiter` | "LF"           | CSV row delimiter: "LF" for Linux, "CR" for MacOs, "CRLF" for Windows (for "CSV" format only). |
| `consumer.gcs.format.csv.object.name`      | ""             | Target GCS object name (for "CSV" format only). |
| `consumer.gcs.format.csv.charset`          | "UTF-8"        | CSV file charset (for "CSV" format only). |
| `consumer.gcs.format.avro.object.name`     | ""             | Target GCS object name (for "AVRO" format only). |
| `consumer.gcs.format.binary.header.name`   | "filename"     | Name of Kafka message header where name of file is stored (for "BINARY" format only). |
| `consumer.gcs.upload.empty.files`          | "true"         | Enable or disable uploading empty files. |
| `consumer.gcs.gcs.project.id`              | ""             | GCS project id. |
| `consumer.gcs.gcs.bucket.name`             | ""             | Target GCS bucket name. |
| `consumer.gcs.gcs.key.file`                | ""             | Key file of GCS service client. |
| `consumer.gcs.gcs.proxy.host`              | ""             | Proxy host. |
| `consumer.gcs.gcs.proxy.port`              | ""             | Proxy port. |
| `consumer.gcs.gcs.proxy.user`              | ""             | Proxy user. |
| `consumer.gcs.gcs.proxy.password`          | ""             | Proxy password.  |
| `consumer.gcs.retry.max.number`            | 3              | Max retry number for committing offset. |
| `consumer.gcs.retry.wait.time.ms`          | 1000           | Retry interval for committing offset.|

#### Splunk Consumer Configs
SplunkConsumer consumes records from a Kafka topic and load them in a Splunk repository.

Additional to "Kafka Consumer Configs" section:

| Config                                   | Default | Description|
|------------------------------------------|---------|------------|
|`consumer.splunk.hec.peers`               | none    | Comma-separated list of HEC (HTTP event collector) peers to connect to (e.g. host1.example.com:8088,host2.example.com:8088) |
|`consumer.splunk.hec.ssl`                 | true    | Flag to indicate if HEC communicates over SSL |
|`consumer.splunk.hec.auth.token`          | none    | Event collector token |
|`consumer.splunk.hec.indexer.ack`         | true    | Flag to indicate if token has indexer acknowledgement enabled |
|`consumer.splunk.hec.raw.endpoint`        | true    | Flag to indicate if the HEC raw endpoint should be used |
|`consumer.splunk.hec.raw.lineBreaker`     | ""      | System dependent line separator (`System.lineSeparator()`) | Event line breaker to use for batched raw events |
|`consumer.splunk.hec.keystore.location`   | none    | Path to the Splunk keystore (only required if `consumer.splunk.hec.ssl=true`) |
|`consumer.splunk.hec.keystore.password`   | none    | Password for the Splunk keystore (only required if `consumer.splunk.hec.ssl=true`) |
|`consumer.splunk.hec.verify.ssl`          | true    | Flag to indicate if SSL hostname verification should be enabled |
|`consumer.splunk.hec.override.index`      | ""      | Overrides the default index |
|`consumer.splunk.hec.override.host`       | ""      | Overrides the default host field |
|`consumer.splunk.hec.override.source`     | ""      | Overrides the default source field |
|`consumer.splunk.hec.override.sourceType` | ""      | Overrides the default sourcetype field |

#### ElasticSearch Consumer Configs
EsConsumer consumes records from a Kafka topic, converts the records to JSON and indexes them to an ElasticSearch index.

Additional to "Kafka Producer Configs" section:

| Config                                       | Default        | Description                                                |
|----------------------------------------------|----------------|------------------------------------------------------------|
| `consumer.es.index.name`                     | ""             | Target ElasticSearch index.                                |
| `consumer.es.format.type`                    | "AVRO"         | Kafka records format ("CSV", "AVRO", "JSON").                                 |
| `consumer.es.connection.address`             | ""             | URL of ElasticSearch server.                               |
| `consumer.es.connection.username`            | ""             | Login for basic authentication on ElasticSearch server.    |
| `consumer.es.connection.password`            | ""             | Password for basic authentication on ElasticSearch server. |
| `consumer.es.connection.truststore.path`     | ""             | Path to truststore for TLS.                                |
| `consumer.es.connection.truststore.password` | ""             | Password for truststore.                                   |
| `consumer.es.consumer.guarantee`             | "EXACTLY_ONCE" | Consumer guarantee ("EXACTLY_ONCE", "AT_LEAST_ONCE").      |
| `consumer.es.consumer.poll.timeout.ms`       | 30000          | Time out for consumer poll request in milliseconds.        |

### Substituting environment variable values in config

This application uses the TypeSafe library for Java to handle configurations. 
The documentation for that library is here: https://github.com/lightbend/config.
In order to substitute environment variables for config values first make sure your file has the `.conf` extension. 
This will make the program treat it as a HOCON file (superset of JSON).
Use the substitution syntax to use environment variable values for configs like so:
```$xslt
my.property.name=${ENV_VAR_NAME}
```
If there is an environment variable set with the name `ENV_VAR_NAME` it will use that value for `my.property.name`. 
And one last note for this syntax - if you're using a '.conf' file you don't normally need double quotes around 
property values UNLESS there's a ':' in the value in which case the whole thing should be wrapped in quotes 
(since the `:` character is reserved).

### Putting it Together
The configuration file is simply a list of key/value pairs in the form of:
```
key=value
```
So for a simple consumer that reads from a topic and writes to a csv file your configuration file might look something like this: 
```
client.type=consumer-db
kafka.common.bootstrap.servers=host.name:port
kafka.common.avro.schema.registry=http://domain.name:port
kafka.common.topic=my.topic
kafka.common.ssl.key.password=myPass
kafka.common.ssl.truststore.password=myPass
kafka.common.ssl.keystore.password=myPass
kafka.common.ssl.truststore.location=/path/to/truststore.jks
kafka.common.ssl.keystore.location=/path/to/keystore.jks
kafka.consumer.group.id=my-group
```

You can use gradle to build the project:
```./gradlew build -x test```

And run using: 
```java -jar -Dconfig.file=/path/to/config.properties my-jar.jar```

This application is still under development and there may be changes in the future. 
A process for running this application in Fargate is also in the works. 
Refer back to this page for the most up to date version/configuration options.  
