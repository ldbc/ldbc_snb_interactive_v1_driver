package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.Time;

public class TimedNamedOperation extends NothingOperation {
    private final String name;

    public TimedNamedOperation(Time startTime, Time dependencyTime, String name) {
        setScheduledStartTime(startTime);
        setDependencyTime(dependencyTime);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "TimedNamedOperation{" +
                "scheduledStartTime=" + scheduledStartTime() +
                ", dependencyTime=" + dependencyTime() +
                ", name='" + name +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimedNamedOperation operation = (TimedNamedOperation) o;

        if (dependencyTime() != null ? !dependencyTime().equals(operation.dependencyTime()) : operation.dependencyTime() != null)
            return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;
        if (scheduledStartTime() != null ? !scheduledStartTime().equals(operation.scheduledStartTime()) : operation.scheduledStartTime() != null)
            return false;

        return true;
    }
}
