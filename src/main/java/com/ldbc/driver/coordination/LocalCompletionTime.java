package com.ldbc.driver.coordination;

import com.ldbc.driver.temporal.Time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class LocalCompletionTime implements CompletionTime {
    private List<Time> initiatedTimes = new ArrayList<Time>();
    private Time maxEventCompletedTime = null;
    private Time completionTime = null;

    private boolean notModifiedSinceLastGet = false;

    @Override
    public void applyInitiatedTime(Time eventInitiatedTime) {
        notModifiedSinceLastGet = false;

        initiatedTimes.add(eventInitiatedTime);
    }

    @Override
    public void applyCompletedTime(Time initiatedTimeOfCompletedEvent) throws CompletionTimeException {
        notModifiedSinceLastGet = false;

        if (false == initiatedTimes.remove(initiatedTimeOfCompletedEvent))
            throw new CompletionTimeException("initiatedTimeOfCompletedEvent does not map to any uncompleted operation");

        if (null == maxEventCompletedTime)
            maxEventCompletedTime = initiatedTimeOfCompletedEvent;
        else if (initiatedTimeOfCompletedEvent.greatThan(maxEventCompletedTime))
            maxEventCompletedTime = initiatedTimeOfCompletedEvent;
    }

    @Override
    public Time get() {
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