package com.ldbc.driver;

import com.ldbc.driver.temporal.Time;

public abstract class Operation<R> {
    private Time scheduledStartTime = null;

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
