package com.ldbc.driver.testutils;

import com.ldbc.driver.temporal.Time;

import java.util.Iterator;

public class TimedNothingOperationFactory implements Iterator<TimedNothingOperation> {
    private final Iterator<Time> startTimes;

    public TimedNothingOperationFactory(Iterator<Time> startTimes) {
        this.startTimes = startTimes;
    }

    @Override
    public boolean hasNext() {
        return startTimes.hasNext();
    }

    @Override
    public TimedNothingOperation next() {
        return new TimedNothingOperation(startTimes.next());
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
