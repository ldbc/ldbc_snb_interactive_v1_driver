package com.ldbc.driver.runtime.metrics;

import com.google.common.collect.Lists;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.runtime.metrics.OperationMetrics.OperationMetricsNameComparator;
import com.ldbc.driver.runtime.metrics.formatters.OperationMetricsFormatter;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import org.apache.log4j.Logger;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class WorkloadMetricsManager {
    private static Logger logger = Logger.getLogger(WorkloadMetricsManager.class);

    public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private static final Duration MINUTES_10 = Duration.fromSeconds(60 * 10);
    public static final Duration DEFAULT_HIGHEST_EXPECTED_DURATION = MINUTES_10;

    private final Map<String, OperationMetrics> allOperationMetrics;

    private final TimeUnit durationUnit;
    private final Duration highestExpectedDuration;
    private Time startTime;
    private Time timeOfLastMeasurement = Time.now();
    private AtomicLong measurementCount = new AtomicLong(0);

    public WorkloadMetricsManager(TimeUnit durationUnit) {
        this(durationUnit, DEFAULT_HIGHEST_EXPECTED_DURATION);
    }

    public WorkloadMetricsManager(TimeUnit durationUnit, Duration highestExpectedDuration) {
        this.durationUnit = durationUnit;
        this.allOperationMetrics = new HashMap<String, OperationMetrics>();
        this.highestExpectedDuration = highestExpectedDuration;
        // TODO perhaps should be initialized to a more explicit time
        startTime = Time.now();
    }

    // TODO this should only be callable once, and should not be set in constructor - revise how startTime is initialized
    public void setStartTime(Time time) {
        startTime = time;
    }

    public void measure(OperationResult operationResult) throws MetricsCollectionException {
        //
        // Number of operations measured
        //
        measurementCount.incrementAndGet();

        //
        // Time last operation was measured
        //
        Time operationEndTime = operationResult.actualStartTime().plus(operationResult.runDuration());
        if (timeOfLastMeasurement.asNano() < operationEndTime.asNano()) {
            timeOfLastMeasurement = operationEndTime;
        }

        OperationMetrics operationMetrics = allOperationMetrics.get(operationResult.operationType());
        if (null == operationMetrics) {
            operationMetrics = new OperationMetrics(operationResult.operationType(), durationUnit,
                    highestExpectedDuration);
        }
        operationMetrics.measure(operationResult);
        allOperationMetrics.put(operationResult.operationType(), operationMetrics);
    }

    public void export(OperationMetricsFormatter metricsFormatter, OutputStream outputStream)
            throws MetricsCollectionException {
        export(metricsFormatter, outputStream, DEFAULT_CHARSET);
    }

    public void export(OperationMetricsFormatter metricsFormatter, OutputStream outputStream, Charset charSet)
            throws MetricsCollectionException {
        try {
            String formattedMetricsGroups = metricsFormatter.format(allOperationMetrics());
            outputStream.write(formattedMetricsGroups.getBytes(charSet));
        } catch (Exception e) {
            String errMsg = "Error encountered writing metrics to output stream";
            logger.error(errMsg);
            throw new MetricsCollectionException(errMsg, e.getCause());
        }
    }

    public List<OperationMetrics> allOperationMetrics() {
        List<OperationMetrics> allOperationMetricsSorted = Lists.newArrayList(allOperationMetrics.values());
        Collections.sort(allOperationMetricsSorted, new OperationMetricsNameComparator());
        return allOperationMetricsSorted;
    }

    public Time startTime() {
        return startTime;
    }

    public String getStatusString() {
        return statusString(Time.now());
    }

    private String statusString(Time atTime) {
        DecimalFormat throughputFormat = new DecimalFormat("#.00");
        return String.format("Status: Runtime (sec) [%s], Operations [%s], Throughput (op/sec) [%s]",
                getElapsedTime(atTime).asSeconds(), measurementCount,
                throughputFormat.format(getThroughputAt(atTime)));
    }

    private double getThroughputAt(Time atTime) {
        return (double) measurementCount.incrementAndGet() / getElapsedTime(atTime).asSeconds();
    }

    private Duration getElapsedTime(Time atTime) {
        return atTime.greaterBy(startTime);
    }
}
