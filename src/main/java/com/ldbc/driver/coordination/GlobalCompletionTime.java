package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;

public class GlobalCompletionTime {
    private final LocalCompletionTime localCompletionTime;
    private final ExternalCompletionTime externalCompletionTime;

    public GlobalCompletionTime(LocalCompletionTime localCompletionTime, ExternalCompletionTime externalCompletionTime) {
        this.localCompletionTime = localCompletionTime;
        this.externalCompletionTime = externalCompletionTime;
    }

    public LocalCompletionTime localCompletionTime() {
        return localCompletionTime;
    }

    public ExternalCompletionTime externalCompletionTime() {
        return externalCompletionTime;
    }

    public Time completionTime() {
        Time localCompletionTimeValue = localCompletionTime.completionTime();
        if (null == localCompletionTimeValue)
            // Until we know what our local completion time is there is no way of knowing what GCT is
            return null;

        Time externalCompletionTimeValue = this.externalCompletionTime.completionTime();
        if (null == externalCompletionTimeValue)
            if (externalCompletionTime.peersIds().isEmpty())
                // There are no peers to hear from -> single instance mode
                return localCompletionTimeValue;
            else
                // One or more of our peers have no replied yet -> no way of knowing what GCT is
                return null;

        // Return min(localCompletionTime,externalCompletionTime)
        return (localCompletionTimeValue.lessThan(externalCompletionTimeValue))
                ? localCompletionTimeValue
                : externalCompletionTimeValue;
    }
}
