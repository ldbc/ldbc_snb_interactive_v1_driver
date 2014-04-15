package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import org.apache.log4j.Logger;

class WorkloadStatusThread extends Thread {
    private static Logger logger = Logger.getLogger(WorkloadStatusThread.class);

    private final Duration statusUpdateInterval;
    private final ConcurrentMetricsService metricsService;
    private final ConcurrentErrorReporter errorReporter;

    WorkloadStatusThread(Duration statusUpdateInterval, ConcurrentMetricsService metricsService, ConcurrentErrorReporter errorReporter) {
        super(WorkloadStatusThread.class.getSimpleName());
        this.statusUpdateInterval = statusUpdateInterval;
        this.metricsService = metricsService;
        this.errorReporter = errorReporter;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(statusUpdateInterval.asMilli());
                String statusString = metricsService.status().toString();
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
