package com.bayer.datahub.libs.services.common.statistics;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.services.common.date.TestDateService;
import com.bayer.datahub.libs.services.common.memory.MemoryService;
import com.bayer.datahub.libs.services.common.notification.NotificationService;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.function.Function;

import static com.bayer.datahub.libs.config.PropertyNames.STATISTICS_INTERMEDIATE_PERIOD_SEC_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

class StatisticsTimerTest {

    @Test
    void printStatisticsIfTime() {
        var dateService = new TestDateService();
        var start = Instant.now();
        var now = start;
        var notificationService = mock(NotificationService.class);
        var periodSec = 30;
        var configs = new Configs(Map.of(STATISTICS_INTERMEDIATE_PERIOD_SEC_PROPERTY, periodSec));
        var timer = new StatisticsTimer(dateService, notificationService, configs);
        var printFunction = new PrintFunction();

        dateService.setNowInstant(now);
        timer.printStatisticsIfTime(printFunction);
        assertThat(printFunction.isInvoked(), equalTo(true));
        assertThat(printFunction.getData(), equalTo(new StatisticsTimer.Data(1, Duration.between(start, now))));

        now = now.plusSeconds(periodSec / 2);
        dateService.setNowInstant(now);
        timer.printStatisticsIfTime(printFunction);
        assertThat(printFunction.isInvoked(), equalTo(false));
        assertThat(printFunction.getData(), nullValue());

        now = now.plusSeconds(periodSec);
        dateService.setNowInstant(now);
        timer.printStatisticsIfTime(printFunction);
        assertThat(printFunction.isInvoked(), equalTo(true));
        assertThat(printFunction.getData(), equalTo(new StatisticsTimer.Data(2, Duration.between(start, now))));

        now = now.plusSeconds(periodSec / 2);
        dateService.setNowInstant(now);
        timer.printStatisticsIfTime(printFunction);
        assertThat(printFunction.isInvoked(), equalTo(false));
        assertThat(printFunction.getData(), nullValue());

        now = now.plusSeconds(periodSec);
        dateService.setNowInstant(now);
        timer.printStatisticsIfTime(printFunction);
        assertThat(printFunction.isInvoked(), equalTo(true));
        assertThat(printFunction.getData(), equalTo(new StatisticsTimer.Data(3, Duration.between(start, now))));
    }

    private static class PrintFunction implements Function<StatisticsTimer.Data, Statistics> {
        private boolean invoked = false;
        private StatisticsTimer.Data data;

        @Override
        public Statistics apply(StatisticsTimer.Data data) {
            invoked = true;
            this.data = data;
            return new StatisticsAggregatorImpl(mock(MemoryService.class)).getStatistics();
        }

        public boolean isInvoked() {
            var i = invoked;
            invoked = false;
            return i;
        }

        public StatisticsTimer.Data getData() {
            var d = data;
            data = null;
            return d;
        }
    }
}