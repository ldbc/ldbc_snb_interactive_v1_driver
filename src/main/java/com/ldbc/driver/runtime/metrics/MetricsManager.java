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
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = Duration.fromMinutes(10);

    private final Time startTime;
    private final Map<String, OperationMetricsManager> allOperationMetrics;
    private final TimeSource TIME_SOURCE;
    private final TimeUnit unit;
    private final Duration highestExpectedDuration;
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
                   Time startTime) {
        this(timeSource, unit, startTime, DEFAULT_HIGHEST_EXPECTED_DURATION);
    }

    MetricsManager(TimeSource timeSource,
                   TimeUnit unit,
                   Time startTime,
                   Duration highestExpectedDuration) {
        this.startTime = startTime;
        this.TIME_SOURCE = timeSource;
        this.unit = unit;
        this.allOperationMetrics = new HashMap<>();
        this.highestExpectedDuration = highestExpectedDuration;
        this.latestFinishTime = startTime;
    }

    void measure(OperationResultReport result) throws MetricsCollectionException {
        Time operationFinishTime = result.actualStartTime().plus(result.runDuration());

        if (null == latestFinishTime)
            latestFinishTime = operationFinishTime;
        else
            latestFinishTime = (operationFinishTime.gt(latestFinishTime)) ? operationFinishTime : latestFinishTime;

        measurementCount.incrementAndGet();

        OperationMetricsManager operationMetricsManager = allOperationMetrics.get(result.operationType());
        if (null == operationMetricsManager)
            operationMetricsManager = new OperationMetricsManager(result.operationType(), unit, highestExpectedDuration);
        operationMetricsManager.measure(result);
        allOperationMetrics.put(result.operationType(), operationMetricsManager);
    }

    Time startTime() {
        return startTime;
    }

    Time latestFinishTime() {
        return latestFinishTime;
    }

    private long totalOperationCount() {
        long count = 0;
        for (OperationMetricsManager operationMetricsManager : allOperationMetrics.values()) {
            count += operationMetricsManager.count();
        }
        return count;
    }

    WorkloadResultsSnapshot snapshot() {
        Map<String, OperationMetricsSnapshot> operationMetricsMap = new HashMap<>();
        for (Map.Entry<String, OperationMetricsManager> metricsManagerEntry : allOperationMetrics.entrySet()) {
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
        Time now = TIME_SOURCE.now();
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
            double operationsPerSecond = ((double) operationCount / runDuration.asNano()) * 1000000000;
            return new WorkloadStatusSnapshot(
                    runDuration,
                    operationCount,
                    durationSinceLastMeasurement,
                    operationsPerSecond);
        }
    }
}
