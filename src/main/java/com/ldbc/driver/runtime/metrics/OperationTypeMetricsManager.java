package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.TemporalUtil;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;

public class OperationTypeMetricsManager {
    private static Logger logger = Logger.getLogger(OperationTypeMetricsManager.class);
    private static final String METRIC_RUNTIME = "Runtime";

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final ContinuousMetricManager runTimeMetric;
    private final String name;
    private final TimeUnit unit;
    private final long highestExpectedRuntimeDurationAsNano;
    private long count = 0;

    OperationTypeMetricsManager(String name,
                                TimeUnit unit,
                                long highestExpectedRuntimeDurationAsNano) {
        this.name = name;
        this.unit = unit;
        this.highestExpectedRuntimeDurationAsNano = highestExpectedRuntimeDurationAsNano;
        this.runTimeMetric = new ContinuousMetricManager(
                METRIC_RUNTIME,
                unit,
                temporalUtil.convert(highestExpectedRuntimeDurationAsNano, TimeUnit.NANOSECONDS, unit),
                4);
    }

    void measure(OperationResultReport operationResultReport) throws MetricsCollectionException {
        //
        // Measure operation runtime
        //
        long runDurationAsNano = operationResultReport.runDurationAsNano();
        if (runDurationAsNano > highestExpectedRuntimeDurationAsNano) {
            String errMsg = String.format(""
                            + "Error recording runtime - reported value exceeds maximum allowed. Time reported as maximum.\n"
                            + "Reported: %s %s / %s\n"
                            + "For: %s\n"
                            + "Maximum: %s %s / %s",
                    operationResultReport.runDurationAsNano(),
                    TimeUnit.NANOSECONDS.name(),
                    temporalUtil.nanoDurationToString(operationResultReport.runDurationAsNano()),
                    name,
                    highestExpectedRuntimeDurationAsNano,
                    TimeUnit.NANOSECONDS.name(),
                    temporalUtil.nanoDurationToString(highestExpectedRuntimeDurationAsNano)
            );
            logger.warn(errMsg);
            runDurationAsNano = highestExpectedRuntimeDurationAsNano;
//            throw new MetricsCollectionException(errMsg);
        }

        long runtimeInAppropriateUnit = temporalUtil.convert(runDurationAsNano, TimeUnit.NANOSECONDS, unit);

        try {
            runTimeMetric.addMeasurement(runtimeInAppropriateUnit);
        } catch (MetricsCollectionException e) {
            String errMsg = String.format("Error encountered adding runtime: %s %s / %s %s\nTo: %s\nHighest expected value: %s %s / %s %s",
                    operationResultReport.runDurationAsNano(),
                    TimeUnit.NANOSECONDS.name(),
                    runtimeInAppropriateUnit,
                    unit.name(),
                    name,
                    highestExpectedRuntimeDurationAsNano,
                    TimeUnit.NANOSECONDS.name(),
                    temporalUtil.convert(highestExpectedRuntimeDurationAsNano, TimeUnit.NANOSECONDS, unit),
                    unit.name()
            );
            throw new MetricsCollectionException(errMsg, e);
        }

        count++;
    }

    public OperationMetricsSnapshot snapshot() {
        return new OperationMetricsSnapshot(name, unit, count(), runTimeMetric.snapshot());
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
