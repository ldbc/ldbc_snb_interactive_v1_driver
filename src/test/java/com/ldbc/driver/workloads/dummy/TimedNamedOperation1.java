package com.ldbc.driver.workloads.dummy;

public class TimedNamedOperation1 extends NothingOperation {
    private final String name;

    public TimedNamedOperation1(long scheduledStartTimeAsMilli, long dependencyTimeAsMilli, String name) {
        setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
        setDependencyTimeAsMilli(dependencyTimeAsMilli);
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

        if (dependencyTimeAsMilli() != operation.dependencyTimeAsMilli()) return false;
        if (scheduledStartTimeAsMilli() != operation.scheduledStartTimeAsMilli()) return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;

        return true;
    }
}
