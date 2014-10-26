package com.ldbc.driver.workloads.dummy;

public class TimedNamedOperation3 extends NothingOperation {
    private final String name;

    public TimedNamedOperation3(long startTime, long dependencyTime, String name) {
        setScheduledStartTimeAsMilli(startTime);
        setDependencyTimeAsMilli(dependencyTime);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "TimedNamedOperation3{" +
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

        TimedNamedOperation3 operation = (TimedNamedOperation3) o;

        if (dependencyTimeAsMilli() != operation.dependencyTimeAsMilli()) return false;
        if (scheduledStartTimeAsMilli() != operation.scheduledStartTimeAsMilli()) return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;

        return true;
    }
}
