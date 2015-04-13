package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.Operation;

import java.util.Iterator;

public class TimedNamedOperation3Factory implements Iterator<Operation> {
    private final Iterator<Long> startTimes;
    private final Iterator<Long> dependencyTimes;
    private final Iterator<String> names;

    public TimedNamedOperation3Factory(Iterator<Long> startTimes,
                                       Iterator<Long> dependencyTimes,
                                       Iterator<String> names) {
        this.startTimes = startTimes;
        this.dependencyTimes = dependencyTimes;
        this.names = names;
    }

    @Override
    public boolean hasNext() {
        return startTimes.hasNext() & dependencyTimes.hasNext();
    }

    @Override
    public TimedNamedOperation3 next() {
        long startTime = startTimes.next();
        return new TimedNamedOperation3(startTime, startTime, dependencyTimes.next(), names.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
