package com.ldbc.driver.workloads.dummy;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TimedNamedOperation1 extends NothingOperation {
    public static final int TYPE = 1;
    public static final String NAME = "name";

    private final String name;

    public TimedNamedOperation1(long scheduledStartTimeAsMilli, long timeStamp, long dependencyTimeAsMilli, String name) {
        setScheduledStartTimeAsMilli(scheduledStartTimeAsMilli);
        setTimeStamp(timeStamp);
        setDependencyTimeStamp(dependencyTimeAsMilli);
        this.name = name;
    }

    public String name() {
        return name;
    }

    @Override
    public Map<String, Object> parameterMap() {
        return ImmutableMap.<String, Object>builder()
                .put(NAME, name)
                .build();
    }

    @Override
    public String toString() {
        return "TimedNamedOperation1{" +
                "scheduledStartTime=" + scheduledStartTimeAsMilli() +
                ", timeStamp=" + timeStamp() +
                ", dependencyTime=" + dependencyTimeStamp() +
                ", name='" + name +
                "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        TimedNamedOperation1 operation = (TimedNamedOperation1) o;

        if (dependencyTimeStamp() != operation.dependencyTimeStamp()) return false;
        if (timeStamp() != operation.timeStamp()) return false;
        if (scheduledStartTimeAsMilli() != operation.scheduledStartTimeAsMilli()) return false;
        if (name != null ? !name.equals(operation.name) : operation.name != null) return false;

        return true;
    }

    @Override
    public int type() {
        return TYPE;
    }
}
