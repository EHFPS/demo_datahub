package com.bayer.datahub.libs.interfaces;

/**
 * Interface that defines consumer and producer lifecycle.
 */
public interface KafkaClient {

    /**
     * Invoked once before the first {@link #run()}.
     */
    void init();

    /**
     * Runs the client. Can be invoked several times.
     */
    void run();

    /**
     * Safely stops the client.
     */
    void stop();
}
