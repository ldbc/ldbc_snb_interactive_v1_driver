package com.ldbc.driver.runtime;

import com.ldbc.driver.runtime.coordination.CompletionTimeService;
import com.ldbc.driver.runtime.metrics.MetricsService.ConcurrentMetricsServiceWriter;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TemporalUtil;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class WorkloadStatusThread extends Thread {
    private static Logger logger = Logger.getLogger(WorkloadStatusThread.class);

    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private static final DecimalFormat OPERATION_COUNT_FORMATTER = new DecimalFormat("###,###,###,###");
    private static final DecimalFormat THROUGHPUT_FORMATTER = new DecimalFormat("###,###,###,##0.00");

    private final long statusUpdateIntervalAsMilli;
    private final ConcurrentMetricsServiceWriter metricsServiceWriter;
    private final ConcurrentErrorReporter errorReporter;
    private final CompletionTimeService completionTimeService;
    private final boolean detailedStatus;
    private AtomicBoolean continueRunning = new AtomicBoolean(true);

    WorkloadStatusThread(long statusUpdateIntervalAsMilli,
                         ConcurrentMetricsServiceWriter metricsServiceWriter,
                         ConcurrentErrorReporter errorReporter,
                         CompletionTimeService completionTimeService,
                         boolean detailedStatus) {
        super(WorkloadStatusThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.statusUpdateIntervalAsMilli = statusUpdateIntervalAsMilli;
        this.metricsServiceWriter = metricsServiceWriter;
        this.errorReporter = errorReporter;
        this.completionTimeService = completionTimeService;
        this.detailedStatus = detailedStatus;
    }

    @Override
    public void run() {
        final RecentThroughputAndDuration recentThroughputAndDuration = new RecentThroughputAndDuration();
        final int statusRecency = 4;
        final long[][] operationCountsAtDurations = new long[statusRecency][2];
        for (int i = 0; i < operationCountsAtDurations.length; i++) {
            operationCountsAtDurations[i][0] = -1;
            operationCountsAtDurations[i][1] = -1;
        }
        int statusRecencyIndex = 0;

        while (continueRunning.get()) {
            try {
                WorkloadStatusSnapshot status = metricsServiceWriter.status();
                operationCountsAtDurations[statusRecencyIndex][0] = status.operationCount();
                operationCountsAtDurations[statusRecencyIndex][1] = status.runDurationAsMilli();
                statusRecencyIndex = (statusRecencyIndex + 1) % statusRecency;

                updateRecentThroughput(operationCountsAtDurations, recentThroughputAndDuration);

                String statusString = (detailedStatus) ?
                        formatWithGct(
                                status.operationCount(),
                                status.runDurationAsMilli(),
                                status.durationSinceLastMeasurementAsMilli(),
                                status.throughput(),
                                recentThroughputAndDuration.throughput(),
                                recentThroughputAndDuration.duration(),
                                completionTimeService.globalCompletionTimeAsMilli()) :
                        formatWithoutGct(
                                status.operationCount(),
                                status.runDurationAsMilli(),
                                status.durationSinceLastMeasurementAsMilli(),
                                status.throughput(),
                                recentThroughputAndDuration.throughput(),
                                recentThroughputAndDuration.duration());

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

    private String formatWithoutGct(long operationCount, long runDurationAsMilli, long durationSinceLastMeasurementAsMilli, double throughput, double recentThroughput, long recentDurationAsMilli) {
        return format(operationCount, runDurationAsMilli, durationSinceLastMeasurementAsMilli, throughput, recentThroughput, recentDurationAsMilli, null).toString();
    }

    private String formatWithGct(long operationCount, long runDurationAsMilli, long durationSinceLastMeasurementAsMilli, double throughput, double recentThroughput, long recentDurationAsMilli, long gctAsMilli) {
        return format(operationCount, runDurationAsMilli, durationSinceLastMeasurementAsMilli, throughput, recentThroughput, recentDurationAsMilli, gctAsMilli).toString();
    }

    private StringBuffer format(long operationCount, long runDurationAsMilli, long durationSinceLastMeasurementAsMilli, double throughput, double recentThroughput, long recentDurationAsMilli, Long gctAsMilli) {
        StringBuffer sb = new StringBuffer();
        sb.append("Runtime [").append((-1 == runDurationAsMilli) ? "--" : TEMPORAL_UTIL.milliDurationToString(runDurationAsMilli)).append("], ");
        sb.append("Operations [").append(OPERATION_COUNT_FORMATTER.format(operationCount)).append("], ");
        sb.append("Last [").append((-1 == durationSinceLastMeasurementAsMilli) ? "--" : TEMPORAL_UTIL.milliDurationToString(durationSinceLastMeasurementAsMilli)).append("], ");
        sb.append("Throughput");
        sb.append(" (Total) [").append(THROUGHPUT_FORMATTER.format(throughput)).append("]");
        sb.append(" (Last ").append(TimeUnit.MILLISECONDS.toSeconds(recentDurationAsMilli)).append("s) [").append(THROUGHPUT_FORMATTER.format(recentThroughput)).append("]");
        if (null != gctAsMilli)
            sb.append(", GCT: " + ((-1 == gctAsMilli) ? "--" : TEMPORAL_UTIL.milliTimeToDateTimeString(gctAsMilli)));
        return sb;
    }

    synchronized public final void shutdown() {
        if (false == continueRunning.get())
            return;
        continueRunning.set(false);
    }

    private void updateRecentThroughput(final long[][] recentOperationCountsAtDurations,
                                        final RecentThroughputAndDuration recentThroughputAndDuration) {
        long minOperationCount = Long.MAX_VALUE;
        long maxOperationCount = Long.MIN_VALUE;
        long minDurationAsMilli = Long.MAX_VALUE;
        long maxDurationAsMilli = Long.MIN_VALUE;
        for (int i = 0; i < recentOperationCountsAtDurations.length; i++) {
            long operationCount = recentOperationCountsAtDurations[i][0];
            long durationAsMilli = recentOperationCountsAtDurations[i][1];
            if (-1 == operationCount) continue;
            minOperationCount = Math.min(minOperationCount, operationCount);
            maxOperationCount = Math.max(maxOperationCount, operationCount);
            minDurationAsMilli = Math.min(minDurationAsMilli, durationAsMilli);
            maxDurationAsMilli = Math.max(maxDurationAsMilli, durationAsMilli);
        }
        long recentRunDurationAsMilli = maxDurationAsMilli - minDurationAsMilli;
        long recentOperationCount = maxOperationCount - minOperationCount;
        double recentThroughput = (0 == recentRunDurationAsMilli)
                ? 0
                : (double) recentOperationCount / recentRunDurationAsMilli * 1000;
        recentThroughputAndDuration.setThroughput(recentThroughput);
        recentThroughputAndDuration.setDuration(recentRunDurationAsMilli);
    }

    private class RecentThroughputAndDuration {
        private double throughput = 0.0;
        private long duration = 0;

        void setThroughput(double throughput) {
            this.throughput = throughput;
        }

        void setDuration(long duration) {
            this.duration = duration;
        }

        double throughput() {
            return throughput;
        }

        long duration() {
            return duration;
        }
    }
}
