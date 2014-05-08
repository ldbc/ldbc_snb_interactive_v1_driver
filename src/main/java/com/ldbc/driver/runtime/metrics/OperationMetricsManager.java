package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResult;
import com.ldbc.driver.temporal.Duration;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class OperationMetricsManager {
    private static final String METRIC_RUNTIME = "Runtime";
    private static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    private static final String METRIC_RESULT_CODE = "Result Code";

    private static final int NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS = 5;

    private ContinuousMetricManager runTimeMetric;
    private ContinuousMetricManager startTimeDelayMetric;
    private DiscreteMetricManager resultCodeMetric;
    private String name;
    private TimeUnit durationUnit;
    private long count = 0;

    OperationMetricsManager(String name, TimeUnit durationUnit, Duration highestExpectedDuration) {
        this.name = name;
        this.durationUnit = durationUnit;
        this.runTimeMetric = new ContinuousMetricManager(METRIC_RUNTIME, durationUnit, highestExpectedDuration.as(durationUnit), NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS);
        this.startTimeDelayMetric = new ContinuousMetricManager(METRIC_START_TIME_DELAY, durationUnit, highestExpectedDuration.as(durationUnit), NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS);
        this.resultCodeMetric = new DiscreteMetricManager(METRIC_RESULT_CODE, "Result Code");
    }

    void measure(OperationResult operationResult) throws MetricsCollectionException {
        //
        // Measure operation runtime
        //
        long runtimeInAppropriateUnit = operationResult.runDuration().as(durationUnit);
        try {
            runTimeMetric.addMeasurement(runtimeInAppropriateUnit);
        } catch (MetricsCollectionException e) {
            String errMsg = String.format("Error encountered adding runtime [%s %s] to [%s]",
                    runtimeInAppropriateUnit, durationUnit.toString(), name);
            throw new MetricsCollectionException(errMsg, e);
        }

        //
        // Measure driver performance - how close is it to target throughput
        //
        Duration startTimeDelay = operationResult.actualStartTime().greaterBy(operationResult.scheduledStartTime());
        long startTimeDelayInAppropriateUnit = startTimeDelay.as(durationUnit);
        try {
            startTimeDelayMetric.addMeasurement(startTimeDelayInAppropriateUnit);
        } catch (MetricsCollectionException e) {
            String errMsg = String.format("Error encountered adding start time delay measurement [%s %s] to [%s]",
                    startTimeDelayInAppropriateUnit, durationUnit.toString(), name);
            throw new MetricsCollectionException(errMsg, e);
        }

        //
        // Measure result code
        //
        int operationResultCode = operationResult.resultCode();
        try {
            resultCodeMetric.addMeasurement(operationResultCode);
        } catch (Exception e) {
            String errMsg = String.format("Error encountered adding result code measurement [%s] to [%s]",
                    operationResultCode, name);
            throw new MetricsCollectionException(errMsg, e);
        }

        count++;
    }

    public OperationMetricsSnapshot snapshot() {
        return new OperationMetricsSnapshot(name, durationUnit, count(), runTimeMetric.snapshot(), startTimeDelayMetric.snapshot(), resultCodeMetric.snapshot());
    }

    public String name() {
        return name;
    }

    public long count() {
        return count;
    }

    static class OperationMetricsNameComparator implements Comparator<OperationMetricsSnapshot> {
        private static final String EMPTY_STRING = "";

        @Override
        public int compare(OperationMetricsSnapshot metrics1, OperationMetricsSnapshot metrics2) {
            String metrics1Name = (metrics1.name() == null) ? EMPTY_STRING : metrics1.name();
            String metrics2Name = (metrics2.name() == null) ? EMPTY_STRING : metrics2.name();
            return metrics1Name.compareTo(metrics2Name);
        }
    }
}
