package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import org.apache.log4j.Logger;

class WorkloadStatusThread extends Thread {
    private static Logger logger = Logger.getLogger(WorkloadStatusThread.class);

    private final Duration statusUpdateInterval;
    private final ConcurrentMetricsService metricsService;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final boolean detailedStatus;

    WorkloadStatusThread(Duration statusUpdateInterval,
                         ConcurrentMetricsService metricsService,
                         ConcurrentErrorReporter errorReporter,
                         ConcurrentCompletionTimeService completionTimeService,
                         boolean detailedStatus) {
        super(WorkloadStatusThread.class.getSimpleName());
        this.statusUpdateInterval = statusUpdateInterval;
        this.metricsService = metricsService;
        this.errorReporter = errorReporter;
        this.completionTimeService = completionTimeService;
        this.detailedStatus = detailedStatus;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(statusUpdateInterval.asMilli());
                String statusString = metricsService.status().toString();
                if (detailedStatus)
                    statusString += ", GCT: " + completionTimeService.globalCompletionTime();
                logger.info(statusString);
            } catch (InterruptedException e) {
                errorReporter.reportError(
                        this,
                        String.format("Status reporting thread was interrupted - exiting\n%s",
                                ConcurrentErrorReporter.stackTraceToString(e)));
                break;
            } catch (Throwable e) {
                errorReporter.reportError(
                        this,
                        String.format("Status reporting thread encountered unexpected error - exiting\n%s",
                                ConcurrentErrorReporter.stackTraceToString(e)));
                break;
            }
        }
    }
}
