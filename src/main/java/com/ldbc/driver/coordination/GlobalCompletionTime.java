package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;

public class GlobalCompletionTime implements CompletionTime {
    private CompletionTime localCompletionTime = new LocalCompletionTime();
    private Time externalCompletionTime = null;

    public void applyExternalCompletionTime(Time latestExternalCompletionTime) {
        externalCompletionTime = latestExternalCompletionTime;
    }

    @Override
    public void applyInitiatedTime(Time eventInitiatedTime) {
        localCompletionTime.applyInitiatedTime(eventInitiatedTime);
    }

    @Override
    public void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException {
        localCompletionTime.applyCompletedTime(initiatedTimeOfCompletedEvent);
    }

    @Override
    public Time get() {
        if (null == externalCompletionTime)
            return null;

        Time localCompletionTimeValue = localCompletionTime.get();

        if (null == localCompletionTimeValue)
            return null;

        return (localCompletionTimeValue.lessThan(externalCompletionTime)) ? localCompletionTimeValue : externalCompletionTime;
    }
}
