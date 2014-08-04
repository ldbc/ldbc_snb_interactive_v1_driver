package com.ldbc.driver.workloads.dummy;

import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;

import java.util.Iterator;

public class TimedNameOperation3Factory implements Iterator<Operation<?>> {
    private final Iterator<Time> startTimes;
    private final Iterator<Time> dependencyTimes;
    private final Iterator<String> names;

    public TimedNameOperation3Factory(Iterator<Time> startTimes,
                                      Iterator<Time> dependencyTimes,
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
        return new TimedNamedOperation3(startTimes.next(), dependencyTimes.next(), names.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
