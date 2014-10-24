package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.temporal.Time;

public class TimedNamedOperation1 extends NothingOperation {
    private final String name;

    public TimedNamedOperation1(Time scheduledStartTime, Time dependencyTime, String name) {
        setScheduledStartTimeAsMilli(scheduledStartTime);
        setDependencyTimeAsMilli(dependencyTime);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "TimedNamedOperation1{" +
                "scheduledStartTime=" + scheduledStartTimeAsMilli() +
                ", dependencyTime=" + dependencyTimeAsMilli() +
                ", name='" + name +
                "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimedNamedOperation1 operation = (TimedNamedOperation1) o;

        if (dependencyTimeAsMilli() != null ? !dependencyTimeAsMilli().equals(operation.dependencyTimeAsMilli()) : operation.dependencyTimeAsMilli() != null)
            return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;
        if (scheduledStartTimeAsMilli() != null ? !scheduledStartTimeAsMilli().equals(operation.scheduledStartTimeAsMilli()) : operation.scheduledStartTimeAsMilli() != null)
            return false;

        return true;
    }
}
