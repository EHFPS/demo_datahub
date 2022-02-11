package com.bayer.datahub.libs.services.common.statistics;

import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.services.common.memory.MemoryService;
import com.bayer.datahub.libs.services.common.memory.TestMemoryService;
import org.junit.jupiter.api.Test;

import java.util.List;

import static com.bayer.datahub.KafkaMetricsHelper.PRODUCER_METRICS_1;
import static com.bayer.datahub.libs.StatisticsAssert.assertStatistics;
import static com.bayer.datahub.libs.StatisticsAssert.sortedMapOf;
import static com.bayer.datahub.libs.services.common.statistics.Statistics.Group.groupOf;
import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;

class StatisticsAggregatorTest {

    @Test
    void getStatisticsEmpty() {
        var statistics = new StatisticsAggregatorImpl(mock(MemoryService.class)).getStatistics();
        assertThat(statistics.getType(), equalTo(Statistics.Type.INTERMEDIATE));
        assertThat(statistics.getStatus(), equalTo(Statistics.Status.SUCCESS));
        assertThat(statistics.getTitle(), nullValue());
        assertThat(statistics.getDescription(), equalTo(" "));
        assertThat(statistics.getSource(), nullValue());
        assertThat(statistics.getGroups(), emptyIterable());
    }

    @Test
    void getStatistics() {
        var title = "the title";
        var description = "the description";
        var performanceGroupName = "Performance";
        var latencyKey = "Latency";
        var latencyValue = "100";
        var counterName1 = "Requests sent";
        var totalMemory = 5_000_000L;
        var freeMemory = 3_000_000L;
        var maxMemory = 7_000_000L;
        var usedMemoryHistoricalMax = 4_000_000L;
        var memoryService = new TestMemoryService(totalMemory, maxMemory, freeMemory, usedMemoryHistoricalMax);

        var source = ClientImpl.class;
        var statistics = new StatisticsAggregatorImpl(memoryService)
                .finalType()
                .title(title)
                .description(description)
                .failed()
                .source(source)
                .incrementCounter(counterName1)
                .addKeyValue(performanceGroupName, latencyKey, latencyValue)
                .addThrowable(new ArithmeticException("Can't divide by zero"))
                .addProducerMetrics(PRODUCER_METRICS_1.metrics())
                .incrementCounter(counterName1)
                .addMemoryUsage()
                .addKeyValue(KAFKA_GROUP, "Records Sent", 77)
                .getStatistics();
        assertThat(statistics.getStatus(), equalTo(Statistics.Status.FAILED));
        assertThat(statistics.getTitle(), equalTo(title));
        assertThat(statistics.getDescription(), equalTo(description));
        assertThat(statistics.getSource(), equalTo(ClientImpl.class));
        assertThat(statistics.getGroups(), hasSize(6));

        assertStatistics(Statistics.Type.FINAL, statistics, title, description, source, Statistics.Status.FAILED,
                List.of(groupOf(EXCEPTION_GROUP, sortedMapOf()),
                        groupOf(KAFKA_GROUP, sortedMapOf()),
                        groupOf(COUNTER_GROUP, sortedMapOf(counterName1, 2)),
                        groupOf(MEMORY_GROUP, sortedMapOf(
                                "Total", "4Mb",
                                "Free", "2Mb",
                                "Max", "6Mb",
                                "Used", "1Mb",
                                "UsedHistoricalMax", "3Mb")),
                        groupOf(performanceGroupName, sortedMapOf()),
                        groupOf(PRODUCER_METRICS_GROUP, sortedMapOf())));
    }

    private abstract static class ClientImpl implements KafkaClient {
    }
}