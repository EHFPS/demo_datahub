package com.bayer.datahub;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;

import java.util.Map;

import static com.bayer.datahub.libs.services.common.statistics.ConsumerMetrics.CONSUMER_METRIC_GROUP;
import static com.bayer.datahub.libs.services.common.statistics.ProducerMetrics.PRODUCER_METRIC_GROUP;

public class KafkaMetricsHelper {
    public static final Metrics PRODUCER_METRICS_1 = new Metrics();
    public static final Metrics CONSUMER_METRICS_1 = new Metrics();
    private static final MetricName MESSAGE_SIZE_AVG_METRIC_NAME = new MetricName("message-size-avg",
            PRODUCER_METRIC_GROUP, "Producer Message Size Average", Map.of());
    private static final MetricName MESSAGE_SIZE_MAX_METRIC_NAME = new MetricName("message-size-max",
            PRODUCER_METRIC_GROUP, "Producer Message Size Max", Map.of());
    private static final MetricName RECORDS_LAG_AVG_METRIC_NAME = new MetricName("records-lag-avg",
            CONSUMER_METRIC_GROUP, "Consumer Records Lag Average", Map.of());
    private static final MetricName RECORDS_LAG_MAX_METRIC_NAME = new MetricName("records-lag-max",
            CONSUMER_METRIC_GROUP, "Consumer Records Lag Max", Map.of());

    static {
        var messageSizesSensor = PRODUCER_METRICS_1.sensor("message-sizes");
        messageSizesSensor.add(MESSAGE_SIZE_AVG_METRIC_NAME, new Avg());
        messageSizesSensor.add(MESSAGE_SIZE_MAX_METRIC_NAME, new Max());
        messageSizesSensor.record(1_000_000);
        messageSizesSensor.record(2_000_000);

        var recordsLagSensor = CONSUMER_METRICS_1.sensor("records-lag");
        recordsLagSensor.add(RECORDS_LAG_AVG_METRIC_NAME, new Avg());
        recordsLagSensor.add(RECORDS_LAG_MAX_METRIC_NAME, new Max());
        recordsLagSensor.record(100);
        recordsLagSensor.record(200);
    }
}
