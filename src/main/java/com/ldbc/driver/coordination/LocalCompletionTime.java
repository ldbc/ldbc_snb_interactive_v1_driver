package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Completion time is the point in time behind which there are no uncompleted events
 * <p/>
 * Completion Time = min( min(Initiated Events), max(Completed Events) )
 */
public class LocalCompletionTime {
    private List<Time> initiatedTimes = new ArrayList<Time>();
    private Time maxEventCompletedTime = null;
    private Time completionTime = null;

    private boolean notModifiedSinceLastGet = false;

    /**
     * Logs the new initiated time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     *
     * @param eventInitiatedTime
     */
    public void applyInitiatedTime(Time eventInitiatedTime) {
        notModifiedSinceLastGet = false;

        initiatedTimes.add(eventInitiatedTime);
    }

    /**
     * Logs the new completed time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     *
     * @param initiatedTimeOfCompletedEvent
     * @throws com.ldbc.driver.coordination.CompletionTimeException
     */
    public void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException {
        notModifiedSinceLastGet = false;

        if (false == initiatedTimes.remove(initiatedTimeOfCompletedEvent))
            throw new CompletionTimeException("initiatedTimeOfCompletedEvent does not map to any uncompleted operation");

        if (null == maxEventCompletedTime)
            maxEventCompletedTime = initiatedTimeOfCompletedEvent;
        else if (initiatedTimeOfCompletedEvent.greatThan(maxEventCompletedTime))
            maxEventCompletedTime = initiatedTimeOfCompletedEvent;
    }

    /**
     * @return min(min(Initiated Events), max(Completed Events) )
     */
    public Time completionTime() {
        if (notModifiedSinceLastGet)
            return completionTime;

        notModifiedSinceLastGet = true;

        if (initiatedTimes.isEmpty()) {
            completionTime = maxEventCompletedTime;
            return completionTime;
        } else if (null == maxEventCompletedTime) {
            return Collections.min(initiatedTimes);
        } else {
            Time minInitiatedTime = Collections.min(initiatedTimes);
            completionTime = (maxEventCompletedTime.lessThan(minInitiatedTime)) ? maxEventCompletedTime : minInitiatedTime;
            return completionTime;
        }
    }
}