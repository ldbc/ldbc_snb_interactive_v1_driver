package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsManager {
    private static final long ONE_SECOND_AS_NANO = 1000000000;

    private final Time startTime;
    private final Map<String, OperationTypeMetricsManager> allOperationMetrics;
    private final TimeSource timeSource;
    private final TimeUnit unit;
    private final Duration highestExpectedRuntimeDuration;
    private final Duration highestExpectedDelayDuration;
    private final boolean recordStartTimeDelayLatency;
    private Time latestFinishTime;
    private AtomicLong measurementCount = new AtomicLong(0);

    public static void export(WorkloadResultsSnapshot workloadResults,
                              OperationMetricsFormatter metricsFormatter,
                              OutputStream outputStream,
                              Charset charSet)
            throws MetricsCollectionException {
        try {
            String formattedMetricsGroups = metricsFormatter.format(workloadResults);
            outputStream.write(formattedMetricsGroups.getBytes(charSet));
        } catch (Exception e) {
            throw new MetricsCollectionException("Error encountered writing metrics to output stream", e);
        }
    }

    MetricsManager(TimeSource timeSource,
                   TimeUnit unit,
                   Time startTime,
                   Duration highestExpectedRuntimeDuration,
                   Duration highestExpectedDelayDuration,
                   boolean recordStartTimeDelayLatency) {
        this.startTime = startTime;
        this.timeSource = timeSource;
        this.unit = unit;
        this.allOperationMetrics = new HashMap<>();
        this.highestExpectedRuntimeDuration = highestExpectedRuntimeDuration;
        this.highestExpectedDelayDuration = highestExpectedDelayDuration;
        this.latestFinishTime = startTime;
        this.recordStartTimeDelayLatency = recordStartTimeDelayLatency;
    }

    void measure(OperationResultReport result) throws MetricsCollectionException {
        Time operationFinishTime = result.actualStartTime().plus(result.runDuration());

        if (null == latestFinishTime)
            latestFinishTime = operationFinishTime;
        else
            latestFinishTime = (operationFinishTime.gt(latestFinishTime)) ? operationFinishTime : latestFinishTime;

        measurementCount.incrementAndGet();

        OperationTypeMetricsManager operationTypeMetricsManager = allOperationMetrics.get(result.operation().type());
        if (null == operationTypeMetricsManager)
            operationTypeMetricsManager = new OperationTypeMetricsManager(
                    result.operation().type(),
                    unit,
                    highestExpectedRuntimeDuration,
                    highestExpectedDelayDuration,
                    recordStartTimeDelayLatency);
        operationTypeMetricsManager.measure(result);
        allOperationMetrics.put(result.operation().type(), operationTypeMetricsManager);
    }

    Time startTime() {
        return startTime;
    }

    Time latestFinishTime() {
        return latestFinishTime;
    }

    private long totalOperationCount() {
        long count = 0;
        for (OperationTypeMetricsManager operationTypeMetricsManager : allOperationMetrics.values()) {
            count += operationTypeMetricsManager.count();
        }
        return count;
    }

    WorkloadResultsSnapshot snapshot() {
        Map<String, OperationMetricsSnapshot> operationMetricsMap = new HashMap<>();
        for (Map.Entry<String, OperationTypeMetricsManager> metricsManagerEntry : allOperationMetrics.entrySet()) {
            operationMetricsMap.put(metricsManagerEntry.getKey(), metricsManagerEntry.getValue().snapshot());
        }
        return new WorkloadResultsSnapshot(
                operationMetricsMap,
                startTime,
                latestFinishTime,
                totalOperationCount(),
                unit);
    }

    WorkloadStatusSnapshot status() {
        Time now = timeSource.now();
        if (now.lt(startTime)) {
            Duration runDuration = Duration.fromMilli(0);
            long operationCount = 0;
            Duration durationSinceLastMeasurement = Duration.fromMilli(0);
            double operationsPerSecond = 0;
            return new WorkloadStatusSnapshot(
                    runDuration,
                    operationCount,
                    durationSinceLastMeasurement,
                    operationsPerSecond);
        } else {
            Duration runDuration = now.durationGreaterThan(startTime);
            long operationCount = measurementCount.get();
            Duration durationSinceLastMeasurement = (null == latestFinishTime) ? null : now.durationGreaterThan(latestFinishTime);
            double operationsPerSecond = ((double) operationCount / runDuration.asNano()) * ONE_SECOND_AS_NANO;
            return new WorkloadStatusSnapshot(
                    runDuration,
                    operationCount,
                    durationSinceLastMeasurement,
                    operationsPerSecond);
        }
    }
}
