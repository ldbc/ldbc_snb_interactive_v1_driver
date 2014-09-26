package com.ldbc.driver.runtime.metrics;

import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.temporal.Time;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class WorkloadResultsSnapshotTest {
    @Test
    public void shouldEqualWhenDifferentSnapshotsHaveSameValues() throws WorkloadException, MetricsCollectionException {
        WorkloadResultsSnapshot snapshot1 = createSnapshot(1, TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        WorkloadResultsSnapshot snapshot2 = createSnapshot(1, TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        assertThat(snapshot1, equalTo(snapshot2));
    }

    @Test
    public void shouldNotEqualWhenDifferentSnapshotsHaveDifferentValues() throws WorkloadException, MetricsCollectionException {
        WorkloadResultsSnapshot snapshot1 = createSnapshot(1, TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        WorkloadResultsSnapshot snapshot2 = createSnapshot(2, TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        assertThat(snapshot1, not(equalTo(snapshot2)));
    }

    @Test
    public void shouldEqualWhenSameSnapshots() throws WorkloadException, MetricsCollectionException {
        WorkloadResultsSnapshot snapshot1 = createSnapshot(1, TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        assertThat(snapshot1, equalTo(snapshot1));
    }

    @Test
    public void shouldStillEqualAfterBeingSerializedAndMarshaledImplementEquals() throws WorkloadException, MetricsCollectionException, IOException {
        WorkloadResultsSnapshot snapshot1 = createSnapshot(1, TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS, TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
        WorkloadResultsSnapshot snapshot2 = WorkloadResultsSnapshot.fromJson(snapshot1.toJson());
        assertThat(snapshot1, equalTo(snapshot2));
    }

    private WorkloadResultsSnapshot createSnapshot(int seed, TimeUnit timeUnit1, TimeUnit timeUnit2, TimeUnit timeUnit3, TimeUnit timeUnit4) {
        String operationName = Integer.toString(seed++);
        TimeUnit operationDurationUnit = timeUnit1;
        long operationCount = seed++;

        String runTimeName = Integer.toString(seed++);
        TimeUnit runTimeUnit = timeUnit2;
        long runTimeCount = seed++;
        double runTimeMean = seed++;
        long runTimeMin = seed++;
        long runTimeMax = seed++;
        long runTimePercentile50 = seed++;
        long runTimePercentile90 = seed++;
        long runTimePercentile95 = seed++;
        long runTimePercentile99 = seed++;
        ContinuousMetricSnapshot runTimeMetric = new ContinuousMetricSnapshot(
                runTimeName,
                runTimeUnit,
                runTimeCount,
                runTimeMean,
                runTimeMin,
                runTimeMax,
                runTimePercentile50,
                runTimePercentile90,
                runTimePercentile95,
                runTimePercentile99);

        String startTimeDelayName = Integer.toString(seed++);
        TimeUnit startTimeDelayUnit = timeUnit3;
        long startTimeDelayCount = seed++;
        double startTimeDelayMean = seed++;
        long startTimeDelayMin = seed++;
        long startTimeDelayMax = seed++;
        long startTimeDelayPercentile50 = seed++;
        long startTimeDelayPercentile90 = seed++;
        long startTimeDelayPercentile95 = seed++;
        long startTimeDelayPercentile99 = seed++;
        ContinuousMetricSnapshot startTimeDelayMetric = new ContinuousMetricSnapshot(
                startTimeDelayName,
                startTimeDelayUnit,
                startTimeDelayCount,
                startTimeDelayMean,
                startTimeDelayMin,
                startTimeDelayMax,
                startTimeDelayPercentile50,
                startTimeDelayPercentile90,
                startTimeDelayPercentile95,
                startTimeDelayPercentile99);

        String resultCodeName = Integer.toString(seed++);
        String resultCodeUnit = Integer.toString(seed++);
        long resultCodeCount = seed++;
        Map<Long, Long> resultCodeAllValues = new HashMap<>();
        resultCodeAllValues.put((long) seed++, (long) seed++);
        DiscreteMetricSnapshot resultCodeMetric = new DiscreteMetricSnapshot(resultCodeName, resultCodeUnit, resultCodeCount, resultCodeAllValues);

        Map<String, OperationMetricsSnapshot> metrics = new HashMap<>();
        metrics.put(Integer.toString(seed++), new OperationMetricsSnapshot(operationName, operationDurationUnit, operationCount, runTimeMetric, startTimeDelayMetric, resultCodeMetric));

        Time operationStartTime = Time.fromMilli(seed++);
        Time operationLatestFinishTime = Time.fromMilli(seed++);
        long count = seed++;
        TimeUnit unit = timeUnit4;
        return new WorkloadResultsSnapshot(metrics, operationStartTime, operationLatestFinishTime, count, unit);
    }
}
