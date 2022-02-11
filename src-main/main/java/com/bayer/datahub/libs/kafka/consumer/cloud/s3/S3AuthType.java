package com.bayer.datahub.libs.kafka.consumer.cloud.s3;

import org.apache.commons.lang3.StringUtils;

public enum S3AuthType {
    AUTO, BASIC, ROLE;

    public static S3AuthType parse(String s) {
        if (StringUtils.isEmpty(s)) {
            throw new IllegalArgumentException("Try to parse empty string to S3AuthType");
        }
        switch (s.toLowerCase()) {
            case "auto":
                return AUTO;
            case "basic":
                return BASIC;
            case "role":
                return ROLE;
            default:
                throw new IllegalArgumentException("Cannot parse S3AuthType: " + s);
        }
    }
}
