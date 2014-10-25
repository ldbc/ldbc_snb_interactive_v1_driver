package com.ldbc.driver.workloads.dummy;

public class TimedNamedOperation2 extends NothingOperation {
    private final String name;

    public TimedNamedOperation2(long startTimeAsMilli, long dependencyTimeAsMilli, String name) {
        setScheduledStartTimeAsMilli(startTimeAsMilli);
        setDependencyTimeAsMilli(dependencyTimeAsMilli);
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

        if (dependencyTimeAsMilli() != operation.dependencyTimeAsMilli()) return false;
        if (scheduledStartTimeAsMilli() != operation.scheduledStartTimeAsMilli()) return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;

        return true;
    }
}
