package com.bayer.datahub.libs.services.common.notification.kafka;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.kafkaclientbuilder.ClientType;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;

import java.util.Map;

import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;

class KafkaStatisticsFormatterTest {

    @Test
    void formatStatistics() {
        var factory = FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, ClientType.PRODUCER_DB_PLAIN.name()))
                .build();
        var statisticsAggregator = factory.getInstance(StatisticsAggregator.class);
        var statistics = statisticsAggregator.finalType().getStatistics();
        var formatter = factory.getInstance(KafkaStatisticsFormatter.class);
        var actJson = formatter.formatStatistics(statistics);
        var expJson = "{\"type\":\"FINAL\",\"title\":null,\"source\":null,\"description\":\" \",\"groups\":[],\"status\":\"SUCCESS\"}";
        JSONAssert.assertEquals(actJson, expJson, false);
    }
}