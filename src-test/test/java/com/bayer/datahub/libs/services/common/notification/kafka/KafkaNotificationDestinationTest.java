package com.bayer.datahub.libs.services.common.notification.kafka;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.KafkaClientHelper;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.kafka.AbstractKafkaBaseTest;
import com.bayer.datahub.libs.services.common.statistics.Statistics;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

class KafkaNotificationDestinationTest extends AbstractKafkaBaseTest {
    private static final KafkaClientHelper KAFKA_CLIENT_HELPER = new KafkaClientHelper(getBrokers(), getSchemaRegistryUrl());

    @Test
    void sendStatistics() {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var factory = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_PLAIN.name(),
                NOTIFICATION_KAFKA_TOPIC_PROPERTY, topic))
                .withStringProducerFactory()
                .withKafkaNotificationProducerFactory()
                .build();
        var statisticsAggregator = factory.getInstance(StatisticsAggregator.class);
        var statisticsIntermediate1 = statisticsAggregator.intermediateType().title("Intermediate1").getStatistics();
        var statisticsIntermediate2 = statisticsAggregator.intermediateType().title("Intermediate2").getStatistics();
        var statisticsFinal = statisticsAggregator.finalType().title("Final").getStatistics();

        var kafkaDestination = factory.getInstance(KafkaNotificationDestination.class);
        kafkaDestination.sendStatistics(statisticsIntermediate1);
        kafkaDestination.sendStatistics(statisticsIntermediate2);
        kafkaDestination.sendStatistics(statisticsFinal);

        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(3));
        var expRecord1 = "{\"type\":\"INTERMEDIATE\",\"title\":\"Intermediate1\",\"source\":null,\"description\":\" \",\"groups\":[],\"status\":\"SUCCESS\"}";
        var expRecord2 = "{\"type\":\"INTERMEDIATE\",\"title\":\"Intermediate2\",\"source\":null,\"description\":\" \",\"groups\":[],\"status\":\"SUCCESS\"}";
        var expRecord3 = "{\"type\":\"FINAL\",\"title\":\"Final\",\"source\":null,\"description\":\" \",\"groups\":[],\"status\":\"SUCCESS\"}";
        var actRecord1 = actRecords.get(0).value();
        var actRecord2 = actRecords.get(1).value();
        var actRecord3 = actRecords.get(2).value();
        assertEquals(actRecord1, expRecord1, false);
        assertEquals(actRecord2, expRecord2, false);
        assertEquals(actRecord3, expRecord3, false);
    }

    @Test
    void sendStatisticsAllowFinalOnly() {
        var topic = KAFKA_CLIENT_HELPER.createRandomTopic();
        var factory = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_PLAIN.name(),
                NOTIFICATION_KAFKA_TOPIC_PROPERTY, topic,
                NOTIFICATION_KAFKA_ALLOWED_STATISTICS_TYPES_BY_COMMA_PROPERTY, Statistics.Type.FINAL.name()))
                .withStringProducerFactory()
                .withKafkaNotificationProducerFactory()
                .build();
        var statisticsAggregator = factory.getInstance(StatisticsAggregator.class);
        var statisticsIntermediate1 = statisticsAggregator.intermediateType().title("Intermediate1").getStatistics();
        var statisticsIntermediate2 = statisticsAggregator.intermediateType().title("Intermediate2").getStatistics();
        var statisticsFinal = statisticsAggregator.finalType().title("Final").getStatistics();

        var kafkaDestination = factory.getInstance(KafkaNotificationDestination.class);
        kafkaDestination.sendStatistics(statisticsIntermediate1);
        kafkaDestination.sendStatistics(statisticsIntermediate2);
        kafkaDestination.sendStatistics(statisticsFinal);

        var actRecords = KAFKA_CLIENT_HELPER.consumeAllStringRecordsFromBeginning(topic);
        assertThat(actRecords, hasSize(1));
        var expRecord1 = "{\"type\":\"FINAL\",\"title\":\"Final\",\"source\":null,\"description\":\" \",\"groups\":[],\"status\":\"SUCCESS\"}";
        var actRecord1 = actRecords.get(0).value();
        assertEquals(actRecord1, expRecord1, false);
    }

}