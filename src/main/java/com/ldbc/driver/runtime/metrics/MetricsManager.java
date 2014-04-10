package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.metrics.OperationMetrics.OperationMetricsNameComparator;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

// TODO test
public class MetricsManager implements WorkloadResults {
    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");
    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = Duration.fromMinutes(10);

    private final Map<String, OperationMetrics> allOperationMetrics;

    private final TimeUnit durationUnit;
    private final Duration highestExpectedDuration;
    private Time earliestStartTime;
    private Time latestFinishTime;
    private AtomicLong measurementCount = new AtomicLong(0);

    public static void export(WorkloadResults workloadResults, OperationMetricsFormatter metricsFormatter, OutputStream outputStream, Charset charSet)
            throws MetricsCollectionException {
        try {
            String formattedMetricsGroups = metricsFormatter.format(workloadResults.metricsForAllOperations());
            outputStream.write(formattedMetricsGroups.getBytes(charSet));
        } catch (Exception e) {
            throw new MetricsCollectionException("Error encountered writing metrics to output stream", e.getCause());
        }
    }

    MetricsManager(TimeUnit durationUnit) {
        this(durationUnit, DEFAULT_HIGHEST_EXPECTED_DURATION);
    }

    MetricsManager(TimeUnit durationUnit, Duration highestExpectedDuration) {
        this.durationUnit = durationUnit;
        this.allOperationMetrics = new HashMap<String, OperationMetrics>();
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

        OperationMetrics operationMetrics = allOperationMetrics.get(result.operationType());
        if (null == operationMetrics)
            operationMetrics = new OperationMetrics(result.operationType(), durationUnit, highestExpectedDuration);
        operationMetrics.measure(result);
        allOperationMetrics.put(result.operationType(), operationMetrics);
    }

    @Override
    public OperationMetrics metricsFor(String operationType) {
        return allOperationMetrics.get(operationType);
    }

    @Override
    public List<OperationMetrics> metricsForAllOperations() {
        List<OperationMetrics> allOperationMetricsSorted = Lists.newArrayList(allOperationMetrics.values());
        Collections.sort(allOperationMetricsSorted, new OperationMetricsNameComparator());
        return allOperationMetricsSorted;
    }

    @Override
    public Time startTime() {
        return earliestStartTime;
    }

    @Override
    public Time finishTime() {
        return latestFinishTime;
    }

    @Override
    public Duration totalRunDuration() {
        return latestFinishTime.greaterBy(earliestStartTime);
    }

    @Override
    public long totalOperationCount() {
        long count = 0;
        for (OperationMetrics operationMetrics : allOperationMetrics.values()) {
            count += operationMetrics.count();
        }
        return count;
    }

    public WorkloadStatus status() {
        Time now = Time.now();
        Duration runDuration = getElapsedTime(now);
        Duration durationSinceLastMeasurement = now.greaterBy(latestFinishTime);
        double operationsPerSecond = getThroughputAt(now);
        return new WorkloadStatus(runDuration, measurementCount.get(), durationSinceLastMeasurement, operationsPerSecond);
    }

    private double getThroughputAt(Time atTime) {
        return (double) measurementCount.get() / getElapsedTime(atTime).asSeconds();
    }

    private Duration getElapsedTime(Time atTime) {
        return atTime.greaterBy(earliestStartTime);
    }

}
