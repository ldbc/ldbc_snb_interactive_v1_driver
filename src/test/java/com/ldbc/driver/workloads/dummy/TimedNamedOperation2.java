package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.temporal.Time;

public class TimedNamedOperation2 extends NothingOperation {
    private final String name;

    public TimedNamedOperation2(Time startTime, Time dependencyTime, String name) {
        setScheduledStartTimeAsMilli(startTime);
        setDependencyTimeAsMilli(dependencyTime);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "TimedNamedOperation2{" +
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

        TimedNamedOperation2 operation = (TimedNamedOperation2) o;

        if (dependencyTimeAsMilli() != null ? !dependencyTimeAsMilli().equals(operation.dependencyTimeAsMilli()) : operation.dependencyTimeAsMilli() != null)
            return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;
        if (scheduledStartTimeAsMilli() != null ? !scheduledStartTimeAsMilli().equals(operation.scheduledStartTimeAsMilli()) : operation.scheduledStartTimeAsMilli() != null)
            return false;

        return true;
    }
}
