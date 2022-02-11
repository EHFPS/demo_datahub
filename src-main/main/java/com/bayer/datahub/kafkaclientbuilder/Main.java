package com.bayer.datahub.kafkaclientbuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        var manager = new ClientManager();
        manager.run();
        log.debug("Exited Main#main()");
    }

}
