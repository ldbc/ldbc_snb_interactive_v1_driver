package com.ldbc.driver.control;

public interface ConcurrentControlService {
    DriverConfiguration configuration();

    void setWorkloadStartTimeAsMilli(long workloadStartTimeAsMilli);

    long workloadStartTimeAsMilli();

    void waitForCommandToExecuteWorkload();

    void waitForAllToCompleteExecutingWorkload();

    void shutdown();
}
