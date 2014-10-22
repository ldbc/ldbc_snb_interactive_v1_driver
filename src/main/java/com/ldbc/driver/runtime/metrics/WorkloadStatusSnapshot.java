package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.temporal.Duration;

import java.text.DecimalFormat;

public class WorkloadStatusSnapshot {
    private final Duration runDuration;
    private final long operationCount;
    private final Duration durationSinceLastMeasurement;
    private final double throughput;
    DecimalFormat operationCountFormatter = new DecimalFormat("###,###,###,###");
    DecimalFormat throughputFormatter = new DecimalFormat("###,###,###,##0.00");

    public WorkloadStatusSnapshot(Duration runDuration, long operationCount, Duration durationSinceLastMeasurement, double throughput) {
        this.runDuration = runDuration;
        this.operationCount = operationCount;
        this.durationSinceLastMeasurement = durationSinceLastMeasurement;
        this.throughput = throughput;
    }

    @Override
    public String toString() {
        return String.format("Runtime [%s], Operations [%s], Since Last Measurement [%s], Throughput (op/sec) [%s]",
                (null == runDuration) ? "--" : runDuration,
                operationCountFormatter.format(operationCount),
                (null == durationSinceLastMeasurement) ? "--" : durationSinceLastMeasurement,
                throughputFormatter.format(throughput));
    }
}
