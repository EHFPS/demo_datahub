# ElasticSearch Consumer  (EsConsumer) requirements

1. Consume all new records from a Kafka topic and index them as documents into an ES index.
1. Supported topic record formats: AVRO
1. Support `EXACTLY_ONCE` and `AT_LEAST_ONCE` guaranties.
1. Compatible with ES v7 or higher (it has not compatible changes: "type removal", 
    [details](https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html)). 
    One index can contain only one mapping.
1. Security
    1. ES user can be specified in `consumer.es.connection.username` and `consumer.es.connection.password` properties.
    1. TLS can be configured in `consumer.es.connection.truststore.path` and `consumer.es.connection.truststore.password`. 
    HTTPS protocol can be chosen in `consumer.es.connection.address` property, e.g. `https://localhost:9200`
1. All available properties:
    1. `consumer.es.index.name`
    1. `consumer.es.connection.address`
    1. `consumer.es.connection.username`
    1. `consumer.es.connection.password`
    1. `consumer.es.connection.truststore.path`
    1. `consumer.es.connection.truststore.password`
    1. `consumer.es.consumer.guarantee`
    1. `consumer.es.consumer.poll.timeout.ms`
