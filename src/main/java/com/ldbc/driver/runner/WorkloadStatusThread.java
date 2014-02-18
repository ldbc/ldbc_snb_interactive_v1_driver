package com.ldbc.driver.runner;

import com.ldbc.driver.metrics.WorkloadMetricsManager;
import com.ldbc.driver.temporal.Duration;
import org.apache.log4j.Logger;

class WorkloadStatusThread extends Thread {
    private static Logger logger = Logger.getLogger(WorkloadStatusThread.class);

    private final Duration statusUpdateInterval;
    private final WorkloadMetricsManager metricsManager;
    private final ConcurrentErrorReporter errorReporter;

    WorkloadStatusThread(Duration statusUpdateInterval, WorkloadMetricsManager metricsManager, ConcurrentErrorReporter errorReporter) {
        this.statusUpdateInterval = statusUpdateInterval;
        this.metricsManager = metricsManager;
        this.errorReporter = errorReporter;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(statusUpdateInterval.asMilli());
                String statusString = metricsManager.getStatusString();
                logger.info(statusString);
            } catch (InterruptedException e) {
                errorReporter.reportError(this, "Status reporting thread was interrupted - exiting");
                break;
            } catch (Exception e) {
                errorReporter.reportError(this, "Status reporting thread encountered unexpected error - exiting");
                break;
            }
        }
    }
}
