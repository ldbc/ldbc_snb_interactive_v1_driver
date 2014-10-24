package com.ldbc.driver.control;

public class LocalControlService implements ConcurrentControlService {
    private final long workloadStartTimeAsMilli;
    private final DriverConfiguration configuration;

    public LocalControlService(long workloadStartTimeAsMilli,
                               DriverConfiguration configuration) {
        this.workloadStartTimeAsMilli = workloadStartTimeAsMilli;
        this.configuration = configuration;
    }

    @Override
    public DriverConfiguration configuration() {
        return configuration;
    }

    @Override
    public long workloadStartTimeAsMilli() {
        return workloadStartTimeAsMilli;
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
        return "Workload Start Time:\t" + workloadStartTimeAsMilli + "\n" +
                configuration.toString();
    }
}
