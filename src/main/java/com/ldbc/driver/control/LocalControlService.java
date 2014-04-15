package com.ldbc.driver.control;

import com.ldbc.driver.temporal.Time;

public class LocalControlService implements ConcurrentControlService {
    private final Time workloadStartTime;
    private final DriverConfiguration configuration;

    public LocalControlService(Time workloadStartTime,
                               DriverConfiguration configuration) {
        this.workloadStartTime = workloadStartTime;
        this.configuration = configuration;
    }

    @Override
    public DriverConfiguration configuration() {
        return configuration;
    }

    @Override
    public Time workloadStartTime() {
        return workloadStartTime;
    }

    @Override
    public void waitForCommandToExecuteWorkload() {

    }

    @Override
    public void waitForAllToCompleteExecutingWorkload() {

    }

    @Override
    public void shutdown() {

    }

    @Override
    public String toString() {
        return "Workload Start Time:\t" + workloadStartTime + "\n" +
                configuration.toString();
    }
}
