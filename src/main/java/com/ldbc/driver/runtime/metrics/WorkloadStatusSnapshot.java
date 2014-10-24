package com.ldbc.driver.runtime.metrics;

import java.text.DecimalFormat;

public class WorkloadStatusSnapshot {
    private final long runDurationAsMilli;
    private final long operationCount;
    private final long durationSinceLastMeasurementAsMilli;
    private final double throughput;
    DecimalFormat operationCountFormatter = new DecimalFormat("###,###,###,###");
    DecimalFormat throughputFormatter = new DecimalFormat("###,###,###,##0.00");

    public WorkloadStatusSnapshot(long runDurationAsMilli,
                                  long operationCount,
                                  long durationSinceLastMeasurementAsMilli,
                                  double throughput) {
        this.runDurationAsMilli = runDurationAsMilli;
        this.operationCount = operationCount;
        this.durationSinceLastMeasurementAsMilli = durationSinceLastMeasurementAsMilli;
        this.throughput = throughput;
    }

    @Override
    public String toString() {
        return String.format("Runtime [%s], Operations [%s], Since Last Measurement [%s], Throughput (op/sec) [%s]",
                (-1 == runDurationAsMilli) ? "--" : runDurationAsMilli,
                operationCountFormatter.format(operationCount),
                (-1 == durationSinceLastMeasurementAsMilli) ? "--" : durationSinceLastMeasurementAsMilli,
                throughputFormatter.format(throughput));
    }
}
