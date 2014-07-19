package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Function0;

public class GlobalCompletionTime {
    private final LocalCompletionTime localCompletionTime;
    private final ExternalCompletionTime externalCompletionTime;
    private final Function0<Time> externalCompletionTimeFun;

    public GlobalCompletionTime(final LocalCompletionTime localCompletionTime, final ExternalCompletionTime externalCompletionTime) {
        this.localCompletionTime = localCompletionTime;
        this.externalCompletionTime = externalCompletionTime;

        if (externalCompletionTime.peersIds().isEmpty())
            // There are no peers to hear from -> single instance mode
            this.externalCompletionTimeFun = new Function0<Time>() {
                @Override
                public Time apply() {
                    return localCompletionTime.completionTime();
                }
            };
        else
            // One or more of our peers have not replied yet -> no way of knowing what GCT is
            this.externalCompletionTimeFun = new Function0<Time>() {
                @Override
                public Time apply() {
                    return externalCompletionTime.completionTime();
                }
            };
    }

    public void applyInitiatedTime(Time eventInitiatedTime) {
        localCompletionTime.applyInitiatedTime(eventInitiatedTime);
    }

    public void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException {
        localCompletionTime.applyCompletedTime(initiatedTimeOfCompletedEvent);
    }

    public void applyPeerCompletionTime(String peerId, Time peerCompletionTime) throws CompletionTimeException {
        externalCompletionTime.applyPeerCompletionTime(peerId, peerCompletionTime);
    }

    public Time completionTime() {
        Time localCompletionTimeValue = localCompletionTime.completionTime();
        if (null == localCompletionTimeValue)
            // Until we know what our local completion time is there is no way of knowing what GCT is
            return null;

        Time externalCompletionTimeValue = externalCompletionTimeFun.apply();
        if (null == externalCompletionTimeValue)
            // One or more of our peers have not replied yet -> no way of knowing what GCT is
            return null;

        // Return min(localCompletionTime,externalCompletionTime)
        return (localCompletionTimeValue.lt(externalCompletionTimeValue))
                ? localCompletionTimeValue
                : externalCompletionTimeValue;
    }
}
