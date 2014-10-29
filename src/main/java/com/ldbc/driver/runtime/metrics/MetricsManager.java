package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;

import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class MetricsManager {
    private static final long ONE_SECOND_AS_NANO = 1000000000;

    private final TemporalUtil temporalUtil = new TemporalUtil();
    private final Map<String, OperationTypeMetricsManager> allOperationMetrics;
    private final TimeSource timeSource;
    private final TimeUnit unit;
    private final long highestExpectedRuntimeDurationAsNano;
    private final long highestExpectedDelayDurationAsMilli;
    private final boolean recordStartTimeDelayLatency;
    private long startTimeAsMilli;
    private long latestFinishTimeAsMilli;
    private long measurementCount = 0;

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
                   long highestExpectedRuntimeDurationAsNano,
                   long highestExpectedDelayDurationAsMilli,
                   boolean recordStartTimeDelayLatency) {
        this.startTimeAsMilli = Long.MAX_VALUE;
        this.latestFinishTimeAsMilli = Long.MIN_VALUE;
        this.timeSource = timeSource;
        this.unit = unit;
        this.allOperationMetrics = new HashMap<>();
        this.highestExpectedRuntimeDurationAsNano = highestExpectedRuntimeDurationAsNano;
        this.highestExpectedDelayDurationAsMilli = highestExpectedDelayDurationAsMilli;
        this.recordStartTimeDelayLatency = recordStartTimeDelayLatency;
    }

    void measure(OperationResultReport result) throws MetricsCollectionException {
        if (result.actualStartTimeAsMilli() < startTimeAsMilli) {
            startTimeAsMilli = result.actualStartTimeAsMilli();
        }

        long operationFinishTimeAsMilli = result.actualStartTimeAsMilli() + temporalUtil.convert(result.runDurationAsNano(), TimeUnit.NANOSECONDS, TimeUnit.MILLISECONDS);
        if (operationFinishTimeAsMilli > latestFinishTimeAsMilli) {
            latestFinishTimeAsMilli = operationFinishTimeAsMilli;
        }

        measurementCount++;

        OperationTypeMetricsManager operationTypeMetricsManager = allOperationMetrics.get(result.operation().type());
        if (null == operationTypeMetricsManager) {
            operationTypeMetricsManager = new OperationTypeMetricsManager(
                    result.operation().type(),
                    unit,
                    temporalUtil.convert(highestExpectedRuntimeDurationAsNano, TimeUnit.NANOSECONDS, unit)
            );
            allOperationMetrics.put(result.operation().type(), operationTypeMetricsManager);
        }
        operationTypeMetricsManager.measure(result);
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
                (startTimeAsMilli == Long.MAX_VALUE) ? -1 : startTimeAsMilli,
                (latestFinishTimeAsMilli == Long.MIN_VALUE) ? -1 : latestFinishTimeAsMilli,
                totalOperationCount(),
                unit);
    }

    WorkloadStatusSnapshot status() {
        long nowAsMilli = timeSource.nowAsMilli();
        if (nowAsMilli < startTimeAsMilli) {
            long runDurationAsMilli = 0;
            long operationCount = 0;
            long durationSinceLastMeasurementAsMilli = 0;
            double operationsPerSecond = 0;
            return new WorkloadStatusSnapshot(
                    runDurationAsMilli,
                    operationCount,
                    durationSinceLastMeasurementAsMilli,
                    operationsPerSecond);
        } else {
            long runDurationAsMilli = nowAsMilli - startTimeAsMilli;
            long operationCount = measurementCount;
            long durationSinceLastMeasurementAsMilli = (-1 == latestFinishTimeAsMilli) ? -1 : nowAsMilli - latestFinishTimeAsMilli;
            double operationsPerSecond = ((double) operationCount / temporalUtil.convert(runDurationAsMilli, TimeUnit.MILLISECONDS, TimeUnit.NANOSECONDS)) * ONE_SECOND_AS_NANO;
            return new WorkloadStatusSnapshot(
                    runDurationAsMilli,
                    operationCount,
                    durationSinceLastMeasurementAsMilli,
                    operationsPerSecond);
        }
    }
}
