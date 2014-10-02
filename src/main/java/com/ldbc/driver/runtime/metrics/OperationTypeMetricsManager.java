package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.Duration;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class OperationTypeMetricsManager {
    private static final String METRIC_RUNTIME = "Runtime";
    private static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    private static final String METRIC_RESULT_CODE = "Result Code";

    private static final int NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS = 5;

    private final ContinuousMetricManager runTimeMetric;
    private final ContinuousMetricManager startTimeDelayMetric;
    private final DiscreteMetricManager resultCodeMetric;
    private final String name;
    private final TimeUnit durationUnit;
    private final boolean recordStartTimeDelayLatency;
    private long count = 0;

    OperationTypeMetricsManager(String name, TimeUnit durationUnit, Duration highestExpectedRuntimeDuration, Duration highestExpectedDelayDuration, boolean recordStartTimeDelayLatency) {
        this.name = name;
        this.durationUnit = durationUnit;
        this.runTimeMetric = new ContinuousMetricManager(METRIC_RUNTIME, durationUnit, highestExpectedRuntimeDuration.as(durationUnit), NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS);
        this.startTimeDelayMetric = new ContinuousMetricManager(METRIC_START_TIME_DELAY, durationUnit, highestExpectedDelayDuration.as(durationUnit), NUMBER_OF_SIGNIFICANT_HDR_HISTOGRAM_DIGITS);
        this.resultCodeMetric = new DiscreteMetricManager(METRIC_RESULT_CODE, "Result Code");
        this.recordStartTimeDelayLatency = recordStartTimeDelayLatency;
    }

    void measure(OperationResultReport operationResultReport) throws MetricsCollectionException {
        //
        // Measure operation runtime
        //
        long runtimeInAppropriateUnit = operationResultReport.runDuration().as(durationUnit);
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
        if (recordStartTimeDelayLatency) {
            Duration startTimeDelay = operationResultReport.actualStartTime().durationGreaterThan(operationResultReport.scheduledStartTime());
            long startTimeDelayInAppropriateUnit = startTimeDelay.as(durationUnit);
            try {
                startTimeDelayMetric.addMeasurement(startTimeDelayInAppropriateUnit);
            } catch (MetricsCollectionException e) {
                String errMsg = String.format("Error encountered adding start time delay measurement [%s %s] to [%s]",
                        startTimeDelayInAppropriateUnit, durationUnit.toString(), name);
                throw new MetricsCollectionException(errMsg, e);
            }
        }

        //
        // Measure result code
        //
        int operationResultCode = operationResultReport.resultCode();
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
