package com.bayer.datahub.libs.exceptions;

public class NullClientException extends RuntimeException {
    public NullClientException() {
        super("Unable to register null as client...");
    }
}
