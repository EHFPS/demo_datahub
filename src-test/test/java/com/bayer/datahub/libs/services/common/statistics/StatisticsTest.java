package com.bayer.datahub.libs.services.common.statistics;

import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StatisticsTest {

    @Test
    void nullType() {
        var e = assertThrows(IllegalStateException.class,
                () -> new Statistics(null, null, null, null, null, null));
        assertThat(e.getMessage(), equalTo("Statistics cannot have type null"));
    }

}