package com.bayer.datahub.libs.exceptions;

public class ValueException extends RuntimeException {
    public ValueException(String methodName, String value, Throwable cause) {
        super("Error calling " + methodName + " with value: " + value, cause);
    }
}
