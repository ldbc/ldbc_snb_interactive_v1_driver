package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TemporalUtil;
import org.apache.log4j.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

class WorkloadStatusThread extends Thread {
    private static Logger logger = Logger.getLogger(WorkloadStatusThread.class);

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final long statusUpdateIntervalAsMilli;
    private final ConcurrentMetricsService metricsService;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentCompletionTimeService concurrentCompletionTimeService;
    private final boolean detailedStatus;
    private AtomicBoolean continueRunning = new AtomicBoolean(true);

    WorkloadStatusThread(long statusUpdateIntervalAsMilli,
                         ConcurrentMetricsService metricsService,
                         ConcurrentErrorReporter errorReporter,
                         ConcurrentCompletionTimeService concurrentCompletionTimeService,
                         boolean detailedStatus) {
        super(WorkloadStatusThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.statusUpdateIntervalAsMilli = statusUpdateIntervalAsMilli;
        this.metricsService = metricsService;
        this.errorReporter = errorReporter;
        this.concurrentCompletionTimeService = concurrentCompletionTimeService;
        this.detailedStatus = detailedStatus;
    }

    @Override
    public void run() {
        while (continueRunning.get()) {
            try {
                String statusString = metricsService.status().toString();
                if (detailedStatus) {
                    long gctAsMilli = concurrentCompletionTimeService.globalCompletionTimeAsMilli();
                    statusString += ", GCT: " + ((-1 == gctAsMilli) ? "--" : temporalUtil.millisecondsToDateTimeString(gctAsMilli));
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
