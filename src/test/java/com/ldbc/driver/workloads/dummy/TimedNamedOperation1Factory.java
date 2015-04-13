package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.Operation;

import java.util.Iterator;

public class TimedNamedOperation1Factory implements Iterator<Operation> {
    private final Iterator<Long> startTimesAsMilli;
    private final Iterator<Long> dependencyTimesAsMilli;
    private final Iterator<String> names;

    public TimedNamedOperation1Factory(Iterator<Long> startTimesAsMilli,
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
    public TimedNamedOperation1 next() {
        long startTime = startTimesAsMilli.next();
        return new TimedNamedOperation1(startTime, startTime, dependencyTimesAsMilli.next(), names.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
