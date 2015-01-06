package com.ldbc.driver.runtime.metrics;

public class WorkloadStatusSnapshot {
    private final long runDurationAsMilli;
    private final long operationCount;
    private final long durationSinceLastMeasurementAsMilli;
    private final double throughput;

    public WorkloadStatusSnapshot(long runDurationAsMilli,
                                  long operationCount,
                                  long durationSinceLastMeasurementAsMilli,
                                  double throughput) {
        this.runDurationAsMilli = runDurationAsMilli;
        this.operationCount = operationCount;
        this.durationSinceLastMeasurementAsMilli = durationSinceLastMeasurementAsMilli;
        this.throughput = throughput;
    }

    public long runDurationAsMilli() {
        return runDurationAsMilli;
    }

    public long operationCount() {
        return operationCount;
    }

    public long durationSinceLastMeasurementAsMilli() {
        return durationSinceLastMeasurementAsMilli;
    }

    public double throughput() {
        return throughput;
    }
}
