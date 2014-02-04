package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;

/**
 * Completion time is the point in time behind which there are no uncompleted events
 * <p/>
 * Completion Time = min( min(Initiated Events), max(Completed Events) )
 * `
 */
public interface CompletionTime {
    /**
     * Logs the new initiated time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     *
     * @param eventInitiatedTime
     */
    public void applyInitiatedTime(Time eventInitiatedTime);

    /**
     * Logs the new completed time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     *
     * @param initiatedTimeOfCompletedEvent
     * @throws com.ldbc.driver.coordination.CompletionTimeException
     */
    public void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException;

    /**
     * @return min(min(Initiated Events), max(Completed Events) )
     */
    public Time get();
}