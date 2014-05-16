package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class MetricsManager {
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = Duration.fromMinutes(10);

    private final Map<String, OperationMetricsManager> allOperationMetrics;

    private final TimeUnit unit;
    private final Duration highestExpectedDuration;
    private Time earliestStartTime;
    private Time latestFinishTime;
    private AtomicLong measurementCount = new AtomicLong(0);

    public static void export(WorkloadResultsSnapshot workloadResults, OperationMetricsFormatter metricsFormatter, OutputStream outputStream, Charset charSet)
            throws MetricsCollectionException {
        try {
            String formattedMetricsGroups = metricsFormatter.format(workloadResults);
            outputStream.write(formattedMetricsGroups.getBytes(charSet));
        } catch (Exception e) {
            throw new MetricsCollectionException("Error encountered writing metrics to output stream", e);
        }
    }

    // TODO take start time in constructor
    MetricsManager(TimeUnit unit) {
        this(unit, DEFAULT_HIGHEST_EXPECTED_DURATION);
    }

    MetricsManager(TimeUnit unit, Duration highestExpectedDuration) {
        this.unit = unit;
        this.allOperationMetrics = new HashMap<String, OperationMetricsManager>();
        this.highestExpectedDuration = highestExpectedDuration;
        earliestStartTime = null;
        latestFinishTime = null;
    }

    void measure(OperationResult result) throws MetricsCollectionException {
        Time operationFinishTime = result.actualStartTime().plus(result.runDuration());

        if (null == earliestStartTime)
            earliestStartTime = result.actualStartTime();
        else
            earliestStartTime = (result.actualStartTime().lt(earliestStartTime)) ? result.actualStartTime() : earliestStartTime;

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
        return earliestStartTime;
    }

    Time finishTime() {
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
        Map<String, OperationMetricsSnapshot> operationMetricsMap = new HashMap<String, OperationMetricsSnapshot>();
        for (Map.Entry<String, OperationMetricsManager> metricsManagerEntry : allOperationMetrics.entrySet()) {
            operationMetricsMap.put(metricsManagerEntry.getKey(), metricsManagerEntry.getValue().snapshot());
        }
        return new WorkloadResultsSnapshot(operationMetricsMap, earliestStartTime, latestFinishTime, totalOperationCount(), unit);
    }

    WorkloadStatus status() {
        // Could also check latest finish time
        if (null == earliestStartTime) return new WorkloadStatus(Duration.fromMilli(0), 0, Duration.fromMilli(0), 0);

        Time now = Time.now();
        Duration runDuration = calculateElapsedTime(now);
        Duration durationSinceLastMeasurement = now.greaterBy(latestFinishTime);
        double operationsPerSecond = calculateThroughputAt(now);
        return new WorkloadStatus(runDuration, measurementCount.get(), durationSinceLastMeasurement, operationsPerSecond);
    }

    private double calculateThroughputAt(Time atTime) {
        return (double) measurementCount.get() / calculateElapsedTime(atTime).asSeconds();
    }

    private Duration calculateElapsedTime(Time atTime) {
        return atTime.greaterBy(earliestStartTime);
    }
}
