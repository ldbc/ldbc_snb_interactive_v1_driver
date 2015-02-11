package com.ldbc.driver.control;

public interface ControlService {
    DriverConfiguration configuration();

    void setWorkloadStartTimeAsMilli(long workloadStartTimeAsMilli);

    long workloadStartTimeAsMilli();

    void waitForCommandToExecuteWorkload();

    void waitForAllToCompleteExecutingWorkload();

    void shutdown();
}
