package com.bayer.datahub.libs.kafka.producer.sharedoc;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.interfaces.Producer;
import com.bayer.datahub.libs.services.common.statistics.StatisticsAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.util.concurrent.ExecutionException;

class ShareDocProducer implements Producer {
    private static final Logger log = LoggerFactory.getLogger(ShareDocProducer.class);
    private final ShareDocService shareDocService;
    private final MetadataProcessor metadataProcessor;
    private final ContentProcessor contentProcessor;
    private final Mode mode;
    private final StatisticsAggregator statisticsAggregator;

    @Inject
    public ShareDocProducer(ShareDocService shareDocService, MetadataProcessor metadataProcessor,
                            ContentProcessor contentProcessor, StatisticsAggregator statisticsAggregator, Configs configs) {
        this.shareDocService = shareDocService;
        this.metadataProcessor = metadataProcessor;
        this.contentProcessor = contentProcessor;
        this.statisticsAggregator = statisticsAggregator;
        mode = Mode.parse(configs.producerSharedocMode);
    }

    @Override
    public void init() {
    }

    @Override
    public void run() {
        try {
            var metadata = shareDocService.loadMetadata();
            var totalLength = metadata.getTotalLength();
            log.debug("Retrieved rows from ShareDoc: {}", totalLength);
            statisticsAggregator.incrementCounter("Received-rows-from-ShareDoc", totalLength);
            if (totalLength == 0) {
                log.info("No rows were fetched from ShareDoc. Nothing to produce to topic.");
                statisticsAggregator.title("Statistics (nothing produce to topic)");
                return;
            }
            var lastKey = shareDocService.findLastModifyDate(metadata);

            switch (mode) {
                case METADATA:
                    metadataProcessor.loadAndProduceMetadata(metadata, lastKey);
                    break;
                case CONTENT:
                    contentProcessor.loadAndProduceContent(metadata, lastKey);
                    break;
                default:
                    throw new IllegalArgumentException("Mode is not supported: " + mode);
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void stop() {
    }
}
