package com.bayer.datahub.libs.exceptions;

public class SchemaMappingError extends RuntimeException {
    public SchemaMappingError(String message, String schema) {
        super(message + "\nSchema: " + schema);
    }
}
