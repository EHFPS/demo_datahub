package com.bayer.datahub.libs.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Retry {
    private static final Logger log = LoggerFactory.getLogger(Retry.class);
    private final int numberOfRetries;
    private int numberOfTriesLeft;
    private final long timeToWait;

    public Retry(int numberOfRetries, long timeToWait) {
        this.numberOfRetries = numberOfRetries;
        numberOfTriesLeft = numberOfRetries;
        this.timeToWait = timeToWait;
    }

    /**
     * @return true if there are tries left
     */
    public boolean shouldRetry() {
        return numberOfTriesLeft > 0;
    }

    private void errorOccurred() throws Exception {
        numberOfTriesLeft--;
        if (!shouldRetry()) {
            throw new Exception("Retry Failed: Total " + numberOfRetries
                    + " attempts made at interval " + timeToWait
                    + "ms");
        }
        waitUntilNextTry();
    }

    private void waitUntilNextTry() {
        try {
            Thread.sleep(timeToWait);
        } catch (InterruptedException ignored) {
        }
    }

    public void retryCatch() {
        try {
            log.warn("Exception occurred ......retrying");
            errorOccurred();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
