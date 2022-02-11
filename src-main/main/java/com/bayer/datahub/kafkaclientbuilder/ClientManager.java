package com.bayer.datahub.kafkaclientbuilder;

import com.bayer.datahub.libs.config.ConfigFileLoader;
import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.KafkaClient;
import com.bayer.datahub.libs.services.common.date.DateService;
import com.bayer.datahub.libs.services.common.memory.MemoryService;
import com.bayer.datahub.libs.services.common.notification.NotificationService;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import com.google.inject.Guice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

import static com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator.TIME_GROUP;
import static com.bayer.datahub.libs.utils.DateHelper.formatDurationHMS;
import static com.bayer.datahub.libs.utils.DateHelper.formatInstant;

class ClientManager {
    private static final Logger log = LoggerFactory.getLogger(ClientManager.class);

    public void run() {
        var configsList = ConfigFileLoader.readConfigsListFromSystemProperty();
        for (Configs configs : configsList) {
            try {
                runClient(configs);
            } catch (Exception e) {
                var strategy = configs.clientErrorStrategy;
                switch (strategy) {
                    case SKIP_CURRENT_CLIENT:
                        log.warn("Skip current client because of exception (strategy=" + strategy + ")", e);
                        break;
                    case SKIP_ALL_CLIENTS:
                        log.error("Skip all clients because of exception (strategy=" + strategy + ")", e);
                        throw e;
                    default:
                        throw new UnsupportedOperationException("Error strategy is not supported: " + strategy);
                }
            }
        }
    }

    private static void runClient(Configs configs) {
        var injector = Guice.createInjector(new AppModule(configs));
        var dateService = injector.getInstance(DateService.class);
        var startInstant = dateService.getNowInstant();
        var memoryService = injector.getInstance(MemoryService.class);
        var statisticsAggregator = injector.getInstance(StatisticsAggregator.class);
        var notificationService = injector.getInstance(NotificationService.class);
        var client = injector.getInstance(KafkaClient.class);
        try {
            memoryService.startMeasureUsedMemory();
            statisticsAggregator.source(client.getClass());
            client.init();
            client.run();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            statisticsAggregator.failed();
            statisticsAggregator.addThrowable(e);
            throw e;
        } finally {
            client.stop();
            var stopInstant = dateService.getNowInstant();
            var duration = Duration.between(startInstant, stopInstant);
            var statistics = statisticsAggregator
                    .addKeyValue(TIME_GROUP, "start_time", formatInstant(startInstant))
                    .addKeyValue(TIME_GROUP, "stop_time", formatInstant(stopInstant))
                    .addKeyValue(TIME_GROUP, "duration", formatDurationHMS(duration))
                    .getStatistics();
            notificationService.sendStatistics(statistics);
        }
    }
}
