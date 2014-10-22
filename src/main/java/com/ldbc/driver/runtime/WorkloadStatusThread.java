package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicBoolean;

class WorkloadStatusThread extends Thread {
    private static Logger logger = Logger.getLogger(WorkloadStatusThread.class);

    private final Duration statusUpdateInterval;
    private final ConcurrentMetricsService metricsService;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentCompletionTimeService concurrentCompletionTimeService;
    private final boolean detailedStatus;
    private AtomicBoolean continueRunning = new AtomicBoolean(true);

    WorkloadStatusThread(Duration statusUpdateInterval,
                         ConcurrentMetricsService metricsService,
                         ConcurrentErrorReporter errorReporter,
                         ConcurrentCompletionTimeService concurrentCompletionTimeService,
                         boolean detailedStatus) {
        super(WorkloadStatusThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.statusUpdateInterval = statusUpdateInterval;
        this.metricsService = metricsService;
        this.errorReporter = errorReporter;
        this.concurrentCompletionTimeService = concurrentCompletionTimeService;
        this.detailedStatus = detailedStatus;
    }

    @Override
    public void run() {
        long statusUpdateIntervalAsMilli = statusUpdateInterval.asMilli();
        while (continueRunning.get()) {
            try {
                String statusString = metricsService.status().toString();
                if (detailedStatus) {
                    Time gct = concurrentCompletionTimeService.globalCompletionTime();
                    statusString += ", GCT: " + ((null == gct) ? "--" : gct.toString());
                }
                logger.info(statusString);
                Spinner.powerNap(statusUpdateIntervalAsMilli);
            } catch (Throwable e) {
                errorReporter.reportError(
                        this,
                        String.format("Status reporting thread encountered unexpected error - exiting\n%s",
                                ConcurrentErrorReporter.stackTraceToString(e)));
                break;
            }
        }
    }

    synchronized public final void shutdown() {
        if (false == continueRunning.get())
            return;
        continueRunning.set(false);
    }
}
