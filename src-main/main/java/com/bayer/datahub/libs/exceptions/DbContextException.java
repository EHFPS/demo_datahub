package com.bayer.datahub.libs.exceptions;

import org.apache.avro.generic.GenericRecord;

import java.util.List;

public class DbContextException extends RuntimeException {
    private final List<GenericRecord> thrownRecords;

    public DbContextException(String message, List<GenericRecord> thrown) {
        super(message);
        thrownRecords = thrown;
    }
}
