package com.bayer.datahub.libs.exceptions;

import org.junit.jupiter.api.Test;

import static com.bayer.datahub.libs.config.PropertyNames.PRODUCER_DB_DELTA_COLUMN_PROPERTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

class InvalidConfigurationExceptionTest {

    @Test
    void messageSettingCurrentValue() {
        var currentValue = "wrongColumn";
        var possibleValues = new Object[]{"columnA", "columnB"};
        var e = new InvalidConfigurationException(PRODUCER_DB_DELTA_COLUMN_PROPERTY, currentValue, possibleValues);
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.db.delta.column'. Current value: 'wrongColumn'. Possible values: 'columnA', 'columnB'. Please check your configuration."));
    }

    @Test
    void messageSettingCurrentValueThrowable() {
        var currentValue = "wrongColumn";
        var possibleValues = new Object[]{"columnA", "columnB"};
        var e = new InvalidConfigurationException(PRODUCER_DB_DELTA_COLUMN_PROPERTY, currentValue, possibleValues);
        assertThat(e.getMessage(), equalTo("Could not find a valid setting for 'producer.db.delta.column'. Current value: 'wrongColumn'. Possible values: 'columnA', 'columnB'. Please check your configuration."));
    }

    @Test
    void messageSettingCurrentValuePossibleValueThrowable() {
        var cause = new NullPointerException("Data is null");
        var currentValue = "wrongColumn";
        var possibleValues = new Object[]{"columnA", "columnB"};
        var e = new InvalidConfigurationException(PRODUCER_DB_DELTA_COLUMN_PROPERTY, currentValue, possibleValues, cause);
        assertThat(e.getMessage(), equalTo(
                "Could not find a valid setting for 'producer.db.delta.column'. Current value: 'wrongColumn'. Possible values: 'columnA', 'columnB'. Please check your configuration."));
        assertThat(e.getCause(), equalTo(cause));
    }

}