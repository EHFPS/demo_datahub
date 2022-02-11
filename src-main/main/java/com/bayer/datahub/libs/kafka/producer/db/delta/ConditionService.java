package com.bayer.datahub.libs.kafka.producer.db.delta;

import com.bayer.datahub.libs.config.Configs;
import com.bayer.datahub.libs.exceptions.InvalidConfigurationException;
import com.bayer.datahub.libs.interfaces.DbContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.Optional;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_COLUMN_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_MIN_OVERRIDE_PROPERTY;
import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_SELECT_INTERVAL_PROPERTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

class ConditionService {
    private static final Logger log = LoggerFactory.getLogger(ConditionService.class);
    private final DbContext dbContext;
    private final DbLatestKeyConsumer dbLatestKeyConsumer;
    private final String deltaColumn;
    private final Duration selectInterval;
    private final LocalDateTime minOverride;
    private final LocalDateTime maxOverride;
    private LocalDateTime iterationMin;
    private LocalDateTime iterationMax;
    private LocalDateTime totalMax;
    private boolean iterationMinIncludes = false;
    private int iterationNum;

    @Inject
    ConditionService(DbContext dbContext, Configs configs, DbLatestKeyConsumer dbLatestKeyConsumer) {
        this.dbContext = dbContext;
        this.dbLatestKeyConsumer = dbLatestKeyConsumer;
        deltaColumn = parseDeltaColumn(configs.producerDbDeltaColumn);
        selectInterval = parseSelectInterval(configs.producerDbDeltaSelectInterval);
        minOverride = DateParser.parse(configs.producerDbDeltaMinOverride);
        maxOverride = DateParser.parse(configs.producerDbDeltaMaxOverride);
    }

    public void init() {
        dbContext.connect();
    }

    public boolean isDone() {
        boolean result;
        if (totalMax == null) {
            result = true;
        } else if (iterationMin == null) {
            result = true;
        } else {
            result = iterationMin.isEqual(totalMax) || iterationMin.isAfter(totalMax);
        }
        log.debug("Is Done: {}", result);
        return result;
    }

    private LocalDateTime findTotalMax() {
        LocalDateTime result;
        var tableMax = dbContext.getMaxValueInColumn(deltaColumn);
        if (tableMax.isPresent()) {
            var tableMaxDate = tableMax.get();
            result = tableMaxDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
            if (maxOverride != null) {
                result = minimum(result, maxOverride);
            }
        } else {
            result = maxOverride;
        }
        log.debug("Total Max: {}", result);
        return result;
    }

    private Duration parseSelectInterval(String producerDbDeltaSelectInterval) {
        Duration result = null;
        if (isNotBlank(producerDbDeltaSelectInterval)) {
            try {
                result = Duration.parse(producerDbDeltaSelectInterval);
            } catch (DateTimeParseException e) {
                throw new InvalidConfigurationException(PRODUCER_DB_DELTA_SELECT_INTERVAL_PROPERTY, e);
            }
        }
        log.debug("Select Interval: {}", result);
        return result;
    }

    private String parseDeltaColumn(String producerDbDeltaColumn) {
        if (isBlank(producerDbDeltaColumn)) {
            throw new InvalidConfigurationException(PRODUCER_DB_DELTA_COLUMN_PROPERTY, producerDbDeltaColumn);
        }
        return producerDbDeltaColumn;
    }

    private LocalDateTime getInitialIterationMin() {
        LocalDateTime initialMin = null;
        var consumedKey = dbLatestKeyConsumer.getLatestKey();
        if (consumedKey != null && !consumedKey.isEmpty()) {
            initialMin = DateParser.parse(consumedKey);
            log.debug("InitialMin in topic: {}", initialMin);
        } else {
            log.debug("InitialMin in topic: not found");
            Optional<? extends Date> minValueInColumn = dbContext.getMinValueInColumn(deltaColumn);
            if (minValueInColumn.isPresent()) {
                var date = minValueInColumn.get();
                initialMin = date.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
                iterationMinIncludes = true;
                log.debug("InitialMin in table: {}", initialMin);
            } else {
                log.debug("InitialMin in table: not found");
            }
        }
        log.debug("InitialMin in {}: {}", PRODUCER_DB_DELTA_MIN_OVERRIDE_PROPERTY, minOverride);
        initialMin = maximum(initialMin, minOverride);
        log.debug("Effective InitialMin: {}", initialMin);
        return initialMin;
    }

    public void stop() {
        dbContext.close();
    }

    private LocalDateTime findIterationMax() {
        LocalDateTime result = null;
        if (selectInterval != null) {
            if (iterationMin != null) {
                result = iterationMin.plus(selectInterval);
                if (result.isAfter(totalMax)) {
                    result = totalMax;
                }
            }
        } else {
            result = totalMax;
        }
        log.debug("Iteration Max: {} ", result);
        return result;
    }

    public LocalDateTime getIterationMin() {
        return iterationMin;
    }

    public LocalDateTime getIterationMax() {
        return iterationMax;
    }

    public boolean isIterationMinIncludes() {
        return iterationMinIncludes;
    }

    public void nextIteration() {
        var isFirstIteration = iterationNum == 0;
        totalMax = findTotalMax();
        if (isFirstIteration) {
            iterationMin = getInitialIterationMin();
        } else {
            iterationMin = iterationMax;
            iterationMinIncludes = false;
        }
        iterationMax = findIterationMax();
        iterationNum++;
        log.info("Next iteration #{}: iterationMin='{}', iterationMax='{}', totalMax='{}'",
                iterationNum, iterationMin, iterationMax, totalMax);
    }

    private LocalDateTime minimum(LocalDateTime localDateTime1, LocalDateTime localDateTime2) {
        if (localDateTime1 == null && localDateTime2 == null) {
            return null;
        }
        if (localDateTime1 == null) {
            return localDateTime2;
        }
        if (localDateTime2 == null) {
            return localDateTime1;
        }
        return localDateTime1.isBefore(localDateTime2) ? localDateTime1 : localDateTime2;
    }

    private LocalDateTime maximum(LocalDateTime localDateTime1, LocalDateTime localDateTime2) {
        if (localDateTime1 == null && localDateTime2 == null) {
            return null;
        }
        if (localDateTime1 == null) {
            return localDateTime2;
        }
        if (localDateTime2 == null) {
            return localDateTime1;
        }
        return localDateTime1.isAfter(localDateTime2) ? localDateTime1 : localDateTime2;
    }
}
