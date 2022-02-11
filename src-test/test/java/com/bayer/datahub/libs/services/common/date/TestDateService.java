package com.bayer.datahub.libs.services.common.date;

import java.time.Instant;
import java.time.LocalDateTime;

public class TestDateService implements DateService {
    private LocalDateTime nowLocalDateTime = LocalDateTime.now();
    private Instant nowInstant = Instant.now();

    @Override
    public LocalDateTime getNowLocalDateTime() {
        return nowLocalDateTime;
    }

    @Override
    public Instant getNowInstant() {
        return nowInstant;
    }

    public void setNowLocalDateTime(LocalDateTime nowLocalDateTime) {
        this.nowLocalDateTime = nowLocalDateTime;
    }

    public void setNowInstant(Instant nowInstant) {
        this.nowInstant = nowInstant;
    }
}
