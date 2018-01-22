package com.ldbc.driver.workloads.dummy;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class TimedNamedOperation2 extends NothingOperation {
    public static final int TYPE = 2;
    public static final String NAME = "name";
    private final String name;

    public TimedNamedOperation2(long startTimeAsMilli, long timeStamp, long dependencyTimeAsMilli, String name) {
        setScheduledStartTimeAsMilli(startTimeAsMilli);
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
        return "TimedNamedOperation2{" +
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

        TimedNamedOperation2 operation = (TimedNamedOperation2) o;

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
