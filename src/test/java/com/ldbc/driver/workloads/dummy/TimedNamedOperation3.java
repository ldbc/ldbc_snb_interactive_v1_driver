package com.ldbc.driver.workloads.dummy;

public class TimedNamedOperation3 extends NothingOperation {
    public static final int TYPE = 3;
    private final String name;

    public TimedNamedOperation3(long scheduledStartTimeAsMilli, long timeStamp, long dependencyTime, String name) {
        setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
        setTimeStamp(timeStamp);
        setDependencyTimeStamp(dependencyTime);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public String toString() {
        return "TimedNamedOperation3{" +
                "scheduledStartTime=" + scheduledStartTimeAsMilli() +
                ", timeStamp=" + timeStamp() +
                ", dependencyTimeStamp=" + dependencyTimeStamp() +
                ", name='" + name +
                "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimedNamedOperation3 operation = (TimedNamedOperation3) o;

        if (timeStamp() != operation.timeStamp()) return false;
        if (dependencyTimeStamp() != operation.dependencyTimeStamp()) return false;
        if (scheduledStartTimeAsMilli() != operation.scheduledStartTimeAsMilli()) return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;

        return true;
    }

    @Override
    public int type() {
        return TYPE;
    }
}
