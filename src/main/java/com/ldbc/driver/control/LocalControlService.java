package com.ldbc.driver.control;

import com.ldbc.driver.temporal.TemporalUtil;

public class LocalControlService implements ConcurrentControlService {
    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final DriverConfiguration configuration;
    private long workloadStartTimeAsMilli;

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
    public void setWorkloadStartTimeAsMilli(long workloadStartTimeAsMilli) {
        this.workloadStartTimeAsMilli = workloadStartTimeAsMilli;
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
        return "Workload Start Time:\t" + temporalUtil.milliTimeToDateTimeString(workloadStartTimeAsMilli) + "\n" +
                configuration.toString();
    }
}
