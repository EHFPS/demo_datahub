# Requirements for "Google Cloud Storage (GCS) Consumer"
1. Transfer not-committed records from a Kafka topic to GCS object(s).
1. Supported formats: AVRO, CSV, BINARY. Configured by `consumer.gcs.format.type` property. Default is `CSV`.
1. For AVRO and CSV formats:
    1. All available Kafka records are saved to single GCS object.
    1. Name of the GCS object is specified in 
    `consumer.gcs.format.avro.object.name` or `consumer.gcs.format.csv.object.name` properties appropriately.
1. For BINARY format:
    1. Each Kafka record is stored in individual GCS object.
    1. Names for GCS objects are taken from Kafka record header having name specified in
    `consumer.gcs.format.csv.header.name` property. Default is `filename`.
1. Supported guaranties: AT_LEAST_ONCE and EXACTLY_ONCE.
1. Credentials (Service Account Key) can be provided in JSON or P12 (PKCS #12) file.
1. Consumed Kafka records are stored on local disk until uploading to GCS.
1. Parallelism:
    1. For AT_LEAST_ONCE guarantee: Kafka records are uploaded to GCS with several concurrent threads. 
    Number of threads is defined in `consumer.gcs.parallelism` property.
    1. For EXACTLY_ONCE guarantee: Kafka records are uploaded to GCS one-by-one.
1. Kafka records having empty value:
    1. For AVRO and CSV formats: ignored.
    1. For BINARY format: uploaded as empty GCS objects (by default). 
    Configurable by `consumer.gcs.format.binary.upload.empty` property.
1. For CSV format:
    1. Value delimiter is specified in `consumer.gcs.format.csv.value.delimiter` property (`,` by default).
    1. Record (row) delimiter is specified in `consumer.gcs.format.csv.record.delimiter`
    (possible values: `LF`, `CR`, `CRLF`; `LF` by default).
    1. Charset is specified in `consumer.gcs.format.csv.charset` property (`UTF-8` by default).
1. Overwrite strategy: OVERWRITE, SKIP, EXCEPTION, APPEND
