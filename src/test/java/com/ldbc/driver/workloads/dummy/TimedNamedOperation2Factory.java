package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.Operation;

import java.util.Iterator;

public class TimedNamedOperation2Factory implements Iterator<Operation> {
    private final Iterator<Long> startTimesAsMilli;
    private final Iterator<Long> dependencyTimesAsMilli;
    private final Iterator<String> names;

    public TimedNamedOperation2Factory(Iterator<Long> startTimesAsMilli,
                                       Iterator<Long> dependencyTimesAsMilli,
                                       Iterator<String> names) {
        this.startTimesAsMilli = startTimesAsMilli;
        this.dependencyTimesAsMilli = dependencyTimesAsMilli;
        this.names = names;
    }

    @Override
    public boolean hasNext() {
        return startTimesAsMilli.hasNext() & dependencyTimesAsMilli.hasNext();
    }

    @Override
    public TimedNamedOperation2 next() {
        long startTime = startTimesAsMilli.next();
        return new TimedNamedOperation2(startTime,startTime, dependencyTimesAsMilli.next(), names.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
