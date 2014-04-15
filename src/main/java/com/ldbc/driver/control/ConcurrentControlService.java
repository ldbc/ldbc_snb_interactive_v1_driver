package com.ldbc.driver.control;

import com.ldbc.driver.temporal.Time;

public interface ConcurrentControlService {
    DriverConfiguration configuration();

    Time workloadStartTime();

    void waitForCommandToExecuteWorkload();

    void waitForAllToCompleteExecutingWorkload();

    void shutdown();
}
