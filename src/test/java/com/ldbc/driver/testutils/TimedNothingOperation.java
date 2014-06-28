package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.Time;

public class TimedNothingOperation extends NothingOperation {
    public TimedNothingOperation(Time startTime) {
        setScheduledStartTime(startTime);
    }
}
