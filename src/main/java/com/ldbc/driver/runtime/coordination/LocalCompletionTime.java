package com.ldbc.driver.runtime.coordination;

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
    void applyInitiatedTime(Time eventInitiatedTime) {
        notModifiedSinceLastGet = false;

        initiatedTimes.add(eventInitiatedTime);
    }

    /**
     * Logs the new completed time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     *
     * @param initiatedTimeOfCompletedEvent
     * @throws com.ldbc.driver.runtime.coordination.CompletionTimeException
     */
    void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException {
        notModifiedSinceLastGet = false;

        if (false == initiatedTimes.remove(initiatedTimeOfCompletedEvent))
            throw new CompletionTimeException("Initiated time of completed event does not map to any uncompleted operation");

        if (null == maxEventCompletedTime)
            maxEventCompletedTime = initiatedTimeOfCompletedEvent;
        else if (initiatedTimeOfCompletedEvent.gt(maxEventCompletedTime))
            maxEventCompletedTime = initiatedTimeOfCompletedEvent;
    }

    /**
     * @return min(min(Initiated Events), max(Completed Events) )
     */
    Time completionTime() {
        if (notModifiedSinceLastGet)
            return completionTime;

        notModifiedSinceLastGet = true;

        if (initiatedTimes.isEmpty()) {
            completionTime = maxEventCompletedTime;
            return completionTime;
        } else if (null == maxEventCompletedTime) {
            return minimumInitiatedTime();
        } else {
            Time minInitiatedTime = Collections.min(initiatedTimes);
            completionTime = (maxEventCompletedTime.lt(minInitiatedTime)) ? maxEventCompletedTime : minInitiatedTime;
            return completionTime;
        }
    }

    private Time minimumInitiatedTime() {
        //TODO Assumes initiated start times are added in ascending order - FAILS if this is not enforced - is there any case where this would not be enforced?
        return initiatedTimes.get(0);
        // return Collections.min(initiatedTimes);
    }
}