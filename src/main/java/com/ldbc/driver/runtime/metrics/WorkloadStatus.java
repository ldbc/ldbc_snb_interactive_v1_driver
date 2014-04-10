package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.temporal.Duration;

import java.text.DecimalFormat;

public class WorkloadStatus {
    private final Duration runDuration;
    private final long operationCount;
    private final Duration durationSinceLastMeasurement;
    private final double throughput;

    public WorkloadStatus(Duration runDuration, long operationCount, Duration durationSinceLastMeasurement, double throughput) {
        this.runDuration = runDuration;
        this.operationCount = operationCount;
        this.durationSinceLastMeasurement = durationSinceLastMeasurement;
        this.throughput = throughput;
    }

    @Override
    public String toString() {
        DecimalFormat throughputFormat = new DecimalFormat("#.00");
        return String.format("Runtime [%s], Operations [%s], Since Last Measurement [%s], Throughput (op/sec) [%s]",
                runDuration,
                operationCount,
                durationSinceLastMeasurement,
                throughputFormat.format(throughput));
    }
}
