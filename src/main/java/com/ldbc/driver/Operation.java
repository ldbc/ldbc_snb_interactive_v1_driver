package com.ldbc.driver;

import com.ldbc.driver.util.temporal.Time;

public abstract class Operation<R> {
    public final static Time UNASSIGNED_SCHEDULED_START_TIME = null;

    private Time scheduledStartTime = UNASSIGNED_SCHEDULED_START_TIME;

    public final void setScheduledStartTime(Time scheduledStartTime) {
        this.scheduledStartTime = scheduledStartTime;
    }

    public final Time scheduledStartTime() {
        return scheduledStartTime;
    }

    public final OperationResult buildResult(int resultCode, R result) {
        return new OperationResult(resultCode, result);
    }

    public String type() {
        return getClass().getName();
    }

    @Override
    public String toString() {
        return String.format("Operation [type=%s, scheduledStartTime=%s]", type(), scheduledStartTime);
    }
}
