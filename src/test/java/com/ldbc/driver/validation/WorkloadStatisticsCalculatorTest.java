package com.ldbc.driver.validation;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.dummy.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WorkloadStatisticsCalculatorTest {
    private TimeSource TIME_SOURCE = new SystemTimeSource();
    private GeneratorFactory gf;

    @Before
    public void init() {
        gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
    }

    @Ignore
    @Test
    public void addMethodForCalculatingMaximumAllowableWindowSize() {
        // TODO only consider SchedulingMode.WINDOW & GctMode.READ/GctMode.READ_WRITE operations as those that are executed in windows
        // TODO all operations in a window must have their dependencies fulfilled before starting
        // TODO GCT only checked at start of window, if at all?
        //
        // TODO... perhaps as part of this exercise the Windowing Algorithm needs to be formally written somewhere,
        // TODO to understand how it now works with the new dependency time concept
        assertThat(true, is(false));
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithSingleOperationType() throws MetricsCollectionException {
        // Given
        Time workloadStartTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Duration operationInterleave = Duration.fromMilli(100);

        Iterator<Operation<?>> operationStreamWithoutTime = gf.limit(new TimedNameOperation1Factory(
                        gf.constantIncrementTime(Time.fromMilli(1), Duration.fromMilli(1)),
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)),
                        gf.constant("name1")
                ),
                operationCount);
        Iterator<Time> operationStartTimes = gf.constantIncrementTime(workloadStartTime, operationInterleave);
        Iterator<Operation<?>> operationStream = gf.assignStartTimes(operationStartTimes, operationStreamWithoutTime);


        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(TimedNamedOperation1.class, new OperationClassification(null, OperationClassification.DependencyMode.NONE));

        // When
        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate(operationStream, operationClassifications, Duration.fromMinutes(60));

        // Then

        // expected values
        long expectedWorkloadDurationAsMilli = (operationCount - 1) * operationInterleave.asMilli();

        assertThat(stats.totalCount(), is(operationCount));
        assertThat(stats.operationTypeCount(), is(1));
        assertThat(stats.totalDuration(), equalTo(Duration.fromMilli(expectedWorkloadDurationAsMilli)));
        assertThat(stats.firstStartTime(), equalTo(workloadStartTime));
        assertThat(stats.lastStartTime(), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));
        assertThat(stats.firstStartTimesByOperationType().get(TimedNamedOperation1.class), equalTo(workloadStartTime));
        assertThat(stats.lastStartTimesByOperationType().get(TimedNamedOperation1.class), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));

        double tolerance = 0.01d;
        Histogram<Class, Double> expectedOperationMix = new Histogram<>(0d);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) TimedNamedOperation1.class), 1d);
        assertThat(
                String.format("Distributions should be within tolerance: %s\n%s\n%s",
                        tolerance,
                        stats.operationMix().toPercentageValues().toPrettyString(),
                        expectedOperationMix.toPercentageValues().toPrettyString()),
                Histogram.equalsWithinTolerance(
                        stats.operationMix().toPercentageValues(),
                        expectedOperationMix.toPercentageValues(),
                        tolerance),
                is(true));

        ContinuousMetricSnapshot operationInterleaves = stats.operationInterleaves().snapshot();
        assertThat(operationInterleaves.min(), equalTo(operationInterleave.asMilli()));
        assertThat(operationInterleaves.mean(), equalTo((double) operationInterleave.asMilli()));
        assertThat(operationInterleaves.percentile95(), equalTo(operationInterleave.asMilli()));
        assertThat(operationInterleaves.max(), equalTo(operationInterleave.asMilli()));
        assertThat(operationInterleaves.count(), equalTo(operationCount - 1));

        ContinuousMetricSnapshot gctNoneInterleaves = stats.operationInterleavesByDependencyMode().get(OperationClassification.DependencyMode.NONE).snapshot();
        assertThat(gctNoneInterleaves.min(), is(operationInterleave.asMilli()));
        assertThat(gctNoneInterleaves.max(), is(operationInterleave.asMilli()));
        assertThat(gctNoneInterleaves.count(), is(operationCount - 1));
        assertThat(gctNoneInterleaves.mean(), is((double) operationInterleave.asMilli()));

        assertThat(stats.operationInterleavesByDependencyMode().get(OperationClassification.DependencyMode.READ), is(nullValue()));

        assertThat(stats.operationInterleavesByDependencyMode().get(OperationClassification.DependencyMode.READ_WRITE), is(nullValue()));

        ContinuousMetricSnapshot operation1Interleaves = stats.operationInterleavesByOperationType().get(TimedNamedOperation1.class).snapshot();
        assertThat(operation1Interleaves.min(), is(operationInterleave.asMilli()));
        assertThat(operation1Interleaves.max(), is(operationInterleave.asMilli()));
        assertThat(operation1Interleaves.count(), is(operationCount - 1));
        assertThat(operation1Interleaves.mean(), is((double) operationInterleave.asMilli()));

        assertThat(stats.operationInterleavesByOperationType().get(TimedNamedOperation2.class), is(nullValue()));

        assertThat(stats.operationInterleavesByOperationType().get(TimedNamedOperation3.class), is(nullValue()));

        assertThat(stats.operationsByDependencyMode().get(OperationClassification.DependencyMode.NONE), equalTo((Set) Sets.newHashSet(TimedNamedOperation1.class)));
        assertThat(stats.operationsByDependencyMode().get(OperationClassification.DependencyMode.READ), is(nullValue()));
        assertThat(stats.operationsByDependencyMode().get(OperationClassification.DependencyMode.READ_WRITE), is(nullValue()));

        System.out.println(stats.toString());
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithMultipleOperationTypes() throws MetricsCollectionException {
        // Given
        long operation1Count = 1000;
        Time operation1StartTime = Time.fromMilli(100);
        Duration operation1Interleave = Duration.fromMilli(100);

        long operation2Count = 100;
        Time operation2StartTime = Time.fromMilli(100);
        Duration operation2Interleave = Duration.fromMilli(1000);

        long operation3Count = 10;
        Time operation3StartTime = Time.fromMilli(100);
        Duration operation3Interleave = Duration.fromMilli(10000);

        Iterator<Operation<?>> operation1Stream = gf.limit(new TimedNameOperation1Factory(
                        gf.constantIncrementTime(operation1StartTime, operation1Interleave),
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(0)),
                        gf.constant("name1")
                ),
                operation1Count);
        Iterator<Operation<?>> operation2Stream = gf.limit(new TimedNameOperation2Factory(
                        gf.constantIncrementTime(operation2StartTime, operation2Interleave),
                        gf.constantIncrementTime(Time.fromMilli(0), operation2Interleave),
                        gf.constant("name2")
                ),
                operation2Count);
        Iterator<Operation<?>> operation3Stream = gf.limit(new TimedNameOperation3Factory(
                        gf.constantIncrementTime(operation3StartTime, operation3Interleave),
                        gf.constantIncrementTime(Time.fromMilli(90), operation3Interleave),
                        gf.constant("name3")
                ),
                operation3Count);

        Iterator<Operation<?>> operationStream = gf.mergeSortOperationsByStartTime(operation1Stream, operation2Stream, operation3Stream);

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(TimedNamedOperation1.class, new OperationClassification(null, OperationClassification.DependencyMode.NONE));
        operationClassifications.put(TimedNamedOperation2.class, new OperationClassification(null, OperationClassification.DependencyMode.READ));
        operationClassifications.put(TimedNamedOperation3.class, new OperationClassification(null, OperationClassification.DependencyMode.READ_WRITE));

        // When
        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate(operationStream, operationClassifications, Duration.fromMinutes(60));

        // Then

        // expected values
        long expectedOperationCount = operation1Count + operation2Count + operation3Count;

        long expectedWorkloadOperation1DurationAsMilli = (operation1Count - 1) * operation1Interleave.asMilli();
        long expectedWorkloadOperation2DurationAsMilli = (operation2Count - 1) * operation2Interleave.asMilli();
        long expectedWorkloadOperation3DurationAsMilli = (operation3Count - 1) * operation3Interleave.asMilli();
        long expectedWorkloadDurationAsMilli =
                Math.max(Math.max(expectedWorkloadOperation1DurationAsMilli, expectedWorkloadOperation2DurationAsMilli), expectedWorkloadOperation3DurationAsMilli);

        assertThat(stats.totalCount(), is(operation1Count + operation2Count + operation3Count));
        assertThat(stats.operationTypeCount(), is(3));
        assertThat(stats.totalDuration(), equalTo(Duration.fromMilli(expectedWorkloadDurationAsMilli)));
        assertThat(stats.firstStartTime(), equalTo(operation1StartTime));
        assertThat(stats.lastStartTime(), equalTo(operation1StartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));
        assertThat(stats.firstStartTimesByOperationType().get(TimedNamedOperation1.class), equalTo(operation1StartTime));
        assertThat(stats.firstStartTimesByOperationType().get(TimedNamedOperation2.class), equalTo(operation2StartTime));
        assertThat(stats.firstStartTimesByOperationType().get(TimedNamedOperation3.class), equalTo(operation3StartTime));
        assertThat(stats.lastStartTimesByOperationType().get(TimedNamedOperation1.class), equalTo(operation1StartTime.plus(Duration.fromMilli(expectedWorkloadOperation1DurationAsMilli))));
        assertThat(stats.lastStartTimesByOperationType().get(TimedNamedOperation2.class), equalTo(operation2StartTime.plus(Duration.fromMilli(expectedWorkloadOperation2DurationAsMilli))));
        assertThat(stats.lastStartTimesByOperationType().get(TimedNamedOperation3.class), equalTo(operation3StartTime.plus(Duration.fromMilli(expectedWorkloadOperation3DurationAsMilli))));

        double tolerance = 0.01d;
        Histogram<Class, Double> expectedOperationMix = new Histogram<>(0d);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) TimedNamedOperation1.class), operation1Count / (double) expectedOperationCount);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) TimedNamedOperation2.class), operation2Count / (double) expectedOperationCount);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) TimedNamedOperation3.class), operation3Count / (double) expectedOperationCount);
        assertThat(
                String.format("Distributions should be within tolerance: %s\n%s\n%s",
                        tolerance,
                        stats.operationMix().toPercentageValues().toPrettyString(),
                        expectedOperationMix.toPercentageValues().toPrettyString()),
                Histogram.equalsWithinTolerance(
                        stats.operationMix().toPercentageValues(),
                        expectedOperationMix.toPercentageValues(),
                        tolerance),
                is(true));

        ContinuousMetricSnapshot gctNoneInterleaves = stats.operationInterleavesByDependencyMode().get(OperationClassification.DependencyMode.NONE).snapshot();
        assertThat(gctNoneInterleaves.min(), is(operation1Interleave.asMilli()));
        assertThat(gctNoneInterleaves.max(), is(operation1Interleave.asMilli()));
        assertThat(gctNoneInterleaves.count(), is(operation1Count - 1));
        assertThat(gctNoneInterleaves.mean(), is((double) operation1Interleave.asMilli()));

        ContinuousMetricSnapshot gctReadInterleaves = stats.operationInterleavesByDependencyMode().get(OperationClassification.DependencyMode.READ).snapshot();
        assertThat(gctReadInterleaves.min(), is(operation2Interleave.asMilli()));
        assertThat(gctReadInterleaves.max(), is(operation2Interleave.asMilli()));
        assertThat(gctReadInterleaves.count(), is(operation2Count - 1));
        assertThat(gctReadInterleaves.mean(), is((double) operation2Interleave.asMilli()));

        ContinuousMetricSnapshot gctReadWriteInterleaves = stats.operationInterleavesByDependencyMode().get(OperationClassification.DependencyMode.READ_WRITE).snapshot();
        assertThat(gctReadWriteInterleaves.min(), is(operation3Interleave.asMilli()));
        assertThat(gctReadWriteInterleaves.max(), is(operation3Interleave.asMilli()));
        assertThat(gctReadWriteInterleaves.count(), is(operation3Count - 1));
        assertThat(gctReadWriteInterleaves.mean(), is((double) operation3Interleave.asMilli()));

        ContinuousMetricSnapshot operation1Interleaves = stats.operationInterleavesByOperationType().get(TimedNamedOperation1.class).snapshot();
        assertThat(operation1Interleaves.min(), is(operation1Interleave.asMilli()));
        assertThat(operation1Interleaves.max(), is(operation1Interleave.asMilli()));
        assertThat(operation1Interleaves.count(), is(operation1Count - 1));
        assertThat(operation1Interleaves.mean(), is((double) operation1Interleave.asMilli()));

        ContinuousMetricSnapshot operation2Interleaves = stats.operationInterleavesByOperationType().get(TimedNamedOperation2.class).snapshot();
        assertThat(operation2Interleaves.min(), is(operation2Interleave.asMilli()));
        assertThat(operation2Interleaves.max(), is(operation2Interleave.asMilli()));
        assertThat(operation2Interleaves.count(), is(operation2Count - 1));
        assertThat(operation2Interleaves.mean(), is((double) operation2Interleave.asMilli()));

        ContinuousMetricSnapshot operation3Interleaves = stats.operationInterleavesByOperationType().get(TimedNamedOperation3.class).snapshot();
        assertThat(operation3Interleaves.min(), is(operation3Interleave.asMilli()));
        assertThat(operation3Interleaves.max(), is(operation3Interleave.asMilli()));
        assertThat(operation3Interleaves.count(), is(operation3Count - 1));
        assertThat(operation3Interleaves.mean(), is((double) operation3Interleave.asMilli()));

        assertThat(stats.operationsByDependencyMode().get(OperationClassification.DependencyMode.NONE), equalTo((Set) Sets.newHashSet(TimedNamedOperation1.class)));
        assertThat(stats.operationsByDependencyMode().get(OperationClassification.DependencyMode.READ), equalTo((Set) Sets.newHashSet(TimedNamedOperation2.class)));
        assertThat(stats.operationsByDependencyMode().get(OperationClassification.DependencyMode.READ_WRITE), equalTo((Set) Sets.newHashSet(TimedNamedOperation3.class)));

        // TODO min (DONE), max, mean
        assertThat(stats.lowestDependencyDurationByOperationType().get(TimedNamedOperation1.class), is(Duration.fromMilli(100)));
        assertThat(stats.lowestDependencyDurationByOperationType().get(TimedNamedOperation2.class), is(Duration.fromMilli(100)));
        assertThat(stats.lowestDependencyDurationByOperationType().get(TimedNamedOperation3.class), is(Duration.fromMilli(10)));

        System.out.println(stats.toString());
    }
}
