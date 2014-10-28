package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class OperationTypeMetricsManager {
    private static final String METRIC_RUNTIME = "Runtime";
    private static final String METRIC_START_TIME_DELAY = "Start Time Delay";
    private static final String METRIC_RESULT_CODE = "Result Code";

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final ContinuousMetricManager runTimeMetric;
    private final ContinuousMetricManager startTimeDelayMetric;
    private final DiscreteMetricManager resultCodeMetric;
    private final String name;
    private final TimeUnit unit;
    private final boolean recordStartTimeDelayLatency;
    private long count = 0;

    OperationTypeMetricsManager(String name,
                                TimeUnit unit,
                                long highestExpectedRuntimeDurationAsNano,
                                long highestExpectedDelayDurationAsMilli,
                                boolean recordStartTimeDelayLatency) {
        this.name = name;
        this.unit = unit;
        this.runTimeMetric = new ContinuousMetricManager(
                METRIC_RUNTIME,
                unit,
                temporalUtil.convert(highestExpectedRuntimeDurationAsNano, TimeUnit.NANOSECONDS, unit),
                4);
        this.startTimeDelayMetric = new ContinuousMetricManager(
                METRIC_START_TIME_DELAY,
                unit,
                temporalUtil.convert(highestExpectedDelayDurationAsMilli, TimeUnit.MILLISECONDS, unit),
                3);
        this.resultCodeMetric = new DiscreteMetricManager(METRIC_RESULT_CODE, "Result Code");
        this.recordStartTimeDelayLatency = recordStartTimeDelayLatency;
    }

    void measure(OperationResultReport operationResultReport) throws MetricsCollectionException {
        //
        // Measure operation runtime
        //
        long runtimeInAppropriateUnit = temporalUtil.convert(operationResultReport.runDurationAsNano(), TimeUnit.NANOSECONDS, unit);
        try {
            runTimeMetric.addMeasurement(runtimeInAppropriateUnit);
        } catch (MetricsCollectionException e) {
            String errMsg = String.format("Error encountered adding runtime [%s %s] to [%s]",
                    runtimeInAppropriateUnit, unit.toString(), name);
            throw new MetricsCollectionException(errMsg, e);
        }

        //
        // Measure driver performance - how close is it to target throughput
        //
        if (recordStartTimeDelayLatency) {
            long startTimeDelayAsMilli = operationResultReport.actualStartTimeAsMilli() - operationResultReport.operation().scheduledStartTimeAsMilli();
            long startTimeDelayInAppropriateUnit = temporalUtil.convert(startTimeDelayAsMilli, TimeUnit.MILLISECONDS, unit);
            try {
                startTimeDelayMetric.addMeasurement(startTimeDelayInAppropriateUnit);
            } catch (Throwable e) {
                String errMsg = String.format("Error encountered adding start time delay measurement [%s %s] to [%s]",
                        startTimeDelayInAppropriateUnit, unit.toString(), name);
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
        return new OperationMetricsSnapshot(name, unit, count(), runTimeMetric.snapshot(), startTimeDelayMetric.snapshot(), resultCodeMetric.snapshot());
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
