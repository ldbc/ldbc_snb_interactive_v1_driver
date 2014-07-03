package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.Time;

public class TimedNothingOperation extends NothingOperation {
    public TimedNothingOperation(Time startTime) {
        setScheduledStartTime(startTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimedNothingOperation that = (TimedNothingOperation) o;
        if (null == that.scheduledStartTime()) return false;
        if (this.scheduledStartTime() == null) return null == that.scheduledStartTime();
        return (this.scheduledStartTime().equals(that.scheduledStartTime()));
    }
}
