package com.ldbc.driver.control;

public interface ConcurrentControlService {
    DriverConfiguration configuration();

    long workloadStartTimeAsMilli();

    void waitForCommandToExecuteWorkload();

    void waitForAllToCompleteExecutingWorkload();

    void shutdown();
}
