package com.ldbc.driver.runtime.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.List;

/**
 * Completion time is the point in time behind which there are no uncompleted events
 * <p/>
 * Approximately --> Completion Time = min( min(Initiated Events), max(Completed Events) )
 */
public class LocalCompletionTime {
    private List<Time> initiatedTimes = new ArrayList<>();
    private Time maxCompletedTime = null;
    private Time completionTime = null;

    /**
     * Logs the new initiated time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     * <p/>
     * NOTE, initiated times MUST be applied in ascending order!
     *
     * @param initiatedTime
     */
    void applyInitiatedTime(Time initiatedTime) {
        initiatedTimes.add(initiatedTime);
    }

    /**
     * Logs the new completed time and updates completion time according to:
     * Completion Time = min(min(Initiated Events), max(Completed Events) )
     *
     * @param initiatedTimeOfCompletedEvent
     * @throws com.ldbc.driver.runtime.coordination.CompletionTimeException
     */
    void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException {
        if (false == initiatedTimes.remove(initiatedTimeOfCompletedEvent))
            throw new CompletionTimeException("Initiated time of completed event does not map to any uncompleted operation");

        if (null == maxCompletedTime)
            maxCompletedTime = initiatedTimeOfCompletedEvent;
        else if (initiatedTimeOfCompletedEvent.gt(maxCompletedTime))
            maxCompletedTime = initiatedTimeOfCompletedEvent;
        updateCompletionTime(initiatedTimeOfCompletedEvent);
    }

    /**
     * @return min(min(Initiated Events), max(Completed Events) )
     */
    Time completionTime() {
        return completionTime;
    }

    private void updateCompletionTime(Time submittedCompletedTime) {
        if (initiatedTimes.isEmpty()) {
            // initiated times [NONE] completed times [?] --> max of completed times -OR- null
            completionTime = maxCompletedTime;
        } else {
            // initiated times [SOME] completed times [SOME]
            Time minInitiatedTime = minimumInitiatedTime();

            if (maxCompletedTime.lt(minInitiatedTime)) {
                completionTime = maxCompletedTime;
            } else if (maxCompletedTime.gt(minInitiatedTime)) {
                if (submittedCompletedTime.lt(minInitiatedTime)) {
                    completionTime = submittedCompletedTime;
                }
            } else {
                // maxCompletedTime == minInitiatedTime
                // not safe to advance LCT
            }
        }
    }

    private Time minimumInitiatedTime() {
        // Assumes initiated start times added in ascending order - FAILS if this is not enforced
        return initiatedTimes.get(0);
    }
}