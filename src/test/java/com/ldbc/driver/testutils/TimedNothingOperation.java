package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.Time;

public class TimedNothingOperation extends NothingOperation {
    public TimedNothingOperation(Time startTime, Time dependencyTime) {
        setScheduledStartTime(startTime);
        setDependencyTime(dependencyTime);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimedNothingOperation operation = (TimedNothingOperation) o;

        if (dependencyTime() != null ? !dependencyTime().equals(operation.dependencyTime()) : operation.dependencyTime() != null)
            return false;
        if (scheduledStartTime() != null ? !scheduledStartTime().equals(operation.scheduledStartTime()) : operation.scheduledStartTime() != null)
            return false;

        return true;
    }
}
