package com.bayer.datahub.libs.services.common.notification.console;

import com.bayer.datahub.FactoryBuilder;
import com.bayer.datahub.libs.services.common.notification.StatisticsFormatter;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

import static com.bayer.datahub.KafkaMetricsHelper.CONSUMER_METRICS_1;
import static com.bayer.datahub.KafkaMetricsHelper.PRODUCER_METRICS_1;
import static com.bayer.datahub.kafkaclientbuilder.ClientType.PRODUCER_DB_PLAIN;
import static com.bayer.datahub.libs.config.PropertyNames.CLIENT_TYPE_PROPERTY;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.TIME_GROUP;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class ConsoleStatisticsFormatterTest {
    private final StatisticsFormatter<String> formatter = new ConsoleStatisticsFormatter();

    @Test
    void formatStatistics() {
        var statistics = initStatisticsAggregator()
                .title("The Title")
                .description("The description")
                .addProducerMetrics(PRODUCER_METRICS_1.metrics())
                .addConsumerMetrics(CONSUMER_METRICS_1.metrics())
                .addThrowable(new IOException("Input/output error."))
                .addThrowable(new FileNotFoundException("The absent file"))
                .addKeyValue(TIME_GROUP, "Duration", "100 sec")
                .addKeyValue(TIME_GROUP, "Start", "10:25:55")
                .addKeyValue(TIME_GROUP, "Stop", "11:35:40")
                .getStatistics();

        var text = formatter.formatStatistics(statistics);
        assertThat(text, equalTo("[FAILED] The Title\n" +
                "Exceptions: {FileNotFoundException=The absent file, IOException=Input/output error.}\n" +
                "Time: {Duration=100 sec, Start=10:25:55, Stop=11:35:40}\n" +
                "Producer Metrics: {message_size_avg=1500000.0, message_size_max=2000000.0}\n" +
                "Consumer Metrics: {records_lag_avg=150.0, records_lag_max=200.0}"));
    }

    private static StatisticsAggregator initStatisticsAggregator() {
        return FactoryBuilder.newBuilder(Map.of(
                CLIENT_TYPE_PROPERTY, PRODUCER_DB_PLAIN.name()))
                .build().getInstance(StatisticsAggregator.class);
    }

    @Test
    void formatStatisticsEmpty() {
        var statistics = initStatisticsAggregator().getStatistics();
        var text = formatter.formatStatistics(statistics);
        assertThat(text, equalTo("[SUCCESS] \n"));
    }
}