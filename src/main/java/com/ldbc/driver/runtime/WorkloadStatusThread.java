package com.ldbc.driver.runtime;

import com.google.common.collect.EvictingQueue;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService.ConcurrentMetricsServiceWriter;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.util.Tuple;
import org.apache.log4j.Logger;

import java.text.DecimalFormat;
import java.util.Queue;
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
    private final ConcurrentCompletionTimeService concurrentCompletionTimeService;
    private final boolean detailedStatus;
    private AtomicBoolean continueRunning = new AtomicBoolean(true);

    WorkloadStatusThread(long statusUpdateIntervalAsMilli,
                         ConcurrentMetricsServiceWriter metricsServiceWriter,
                         ConcurrentErrorReporter errorReporter,
                         ConcurrentCompletionTimeService concurrentCompletionTimeService,
                         boolean detailedStatus) {
        super(WorkloadStatusThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.statusUpdateIntervalAsMilli = statusUpdateIntervalAsMilli;
        this.metricsServiceWriter = metricsServiceWriter;
        this.errorReporter = errorReporter;
        this.concurrentCompletionTimeService = concurrentCompletionTimeService;
        this.detailedStatus = detailedStatus;
    }

    @Override
    public void run() {
        EvictingQueue<OperationCountAtDuration> operationCountsAtDurations = EvictingQueue.create(5);
        while (continueRunning.get()) {
            try {
                WorkloadStatusSnapshot status = metricsServiceWriter.status();
                operationCountsAtDurations.add(
                        new OperationCountAtDuration(status.operationCount(), status.runDurationAsMilli())
                );
                Tuple.Tuple2<Double, Long> recentThroughput = recentThroughput(operationCountsAtDurations);

                String statusString = (detailedStatus) ?
                        formatWithGct(
                                status.operationCount(),
                                status.runDurationAsMilli(),
                                status.durationSinceLastMeasurementAsMilli(),
                                status.throughput(),
                                recentThroughput._1(),
                                recentThroughput._2(),
                                concurrentCompletionTimeService.globalCompletionTimeAsMilli()) :
                        formatWithoutGct(
                                status.operationCount(),
                                status.runDurationAsMilli(),
                                status.durationSinceLastMeasurementAsMilli(),
                                status.throughput(),
                                recentThroughput._1(),
                                recentThroughput._2());

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

    private Tuple.Tuple2<Double, Long> recentThroughput(Queue<OperationCountAtDuration> recentOperationCountsAtDurations) {
        long minOperationCount = Long.MAX_VALUE;
        long maxOperationCount = Long.MIN_VALUE;
        long minDurationAsMilli = Long.MAX_VALUE;
        long maxDurationAsMilli = Long.MIN_VALUE;
        for (OperationCountAtDuration operationCountAtDuration : recentOperationCountsAtDurations) {
            long operationCount = operationCountAtDuration.operationCount();
            long durationAsMilli = operationCountAtDuration.durationAsMilli();
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
        return Tuple.tuple2(recentThroughput, recentRunDurationAsMilli);
    }

    private class OperationCountAtDuration {
        private final long operationCount;
        private final long durationAsMilli;

        private OperationCountAtDuration(long operationCount, long durationAsMilli) {
            this.operationCount = operationCount;
            this.durationAsMilli = durationAsMilli;
        }

        public long operationCount() {
            return operationCount;
        }

        public long durationAsMilli() {
            return durationAsMilli;
        }
    }
}
