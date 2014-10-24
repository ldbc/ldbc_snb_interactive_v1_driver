package com.ldbc.driver.validation;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.workloads.dummy.*;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WorkloadStatisticsCalculatorTest {
    private GeneratorFactory gf;

    @Before
    public void init() {
        gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithSingleOperationType() throws MetricsCollectionException {
        // Given
        Time workloadStartTime = Time.fromMilli(0);
        long operationCount = 1000;
        Duration operationInterleave = Duration.fromMilli(100);

        Iterator<Operation<?>> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        gf.constantIncrementTime(workloadStartTime, operationInterleave),
                        gf.constantIncrementTime(workloadStartTime.minus(operationInterleave), operationInterleave),
                        gf.constant("name1")
                ),
                operationCount);

        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                Sets.<Class<? extends Operation<?>>>newHashSet(),
                Collections.<Operation<?>>emptyIterator(),
                operations
        );

        // When

        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate(workloadStreams, Duration.fromMinutes(60));

        // Then

        // expected values
        long expectedWorkloadDurationAsMilli = (operationCount - 1) * operationInterleave.asMilli();

        assertThat(stats.totalCount(), is(operationCount));
        assertThat(stats.operationTypeCount(), is(1));
        assertThat(stats.totalDurationAsMilli(), equalTo(Duration.fromMilli(expectedWorkloadDurationAsMilli)));
        assertThat(stats.firstStartTimeAsMilli(), equalTo(workloadStartTime));
        assertThat(stats.lastStartTimeAsMilli(), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));
        assertThat(stats.firstStartTimesAsMilliByOperationType().get(TimedNamedOperation1.class), equalTo(workloadStartTime));
        assertThat(stats.lastStartTimesAsMilliByOperationType().get(TimedNamedOperation1.class), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));

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

        assertThat(stats.interleavesForDependentOperations().snapshot().count(), is(0l));
        assertThat(stats.interleavesForDependencyOperations().snapshot().count(), is(0l));

        ContinuousMetricSnapshot operation1Interleaves = stats.operationInterleavesByOperationType().get(TimedNamedOperation1.class).snapshot();
        assertThat(operation1Interleaves.min(), is(operationInterleave.asMilli()));
        assertThat(operation1Interleaves.max(), is(operationInterleave.asMilli()));
        assertThat(operation1Interleaves.count(), is(operationCount - 1));
        assertThat(operation1Interleaves.mean(), is((double) operationInterleave.asMilli()));

        assertThat(stats.operationInterleavesByOperationType().get(TimedNamedOperation2.class), is(nullValue()));

        assertThat(stats.operationInterleavesByOperationType().get(TimedNamedOperation3.class), is(nullValue()));

        assertThat(stats.dependencyOperationTypes(), equalTo((Set) new HashSet<Class>()));
        assertThat(stats.dependentOperationTypes(), equalTo((Set) new HashSet<Class>()));

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

        Iterator<Operation<?>> operation1Stream = gf.limit(
                new TimedNamedOperation1Factory(
                        gf.constantIncrementTime(operation1StartTime, operation1Interleave),
                        gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(0)),
                        gf.constant("name1")
                ),
                operation1Count);
        Iterator<Operation<?>> operation2Stream = gf.limit(
                new TimedNamedOperation2Factory(
                        gf.constantIncrementTime(operation2StartTime, operation2Interleave),
                        gf.constantIncrementTime(Time.fromMilli(0), operation2Interleave),
                        gf.constant("name2")
                ),
                operation2Count);
        Iterator<Operation<?>> operation3Stream = gf.limit(
                new TimedNamedOperation3Factory(
                        gf.constantIncrementTime(operation3StartTime, operation3Interleave),
                        gf.constantIncrementTime(operation3StartTime.minus(Duration.fromMilli(10)), operation3Interleave),
                        gf.constant("name3")
                ),
                operation3Count);

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation<?>>> dependentOperations = Sets.<Class<? extends Operation<?>>>newHashSet(
                TimedNamedOperation2.class,
                TimedNamedOperation3.class
        );
        workloadStreams.setAsynchronousStream(
                dependentOperations,
                operation3Stream,
                gf.mergeSortOperationsByStartTime(operation1Stream, operation2Stream)
        );

        // When

        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate(workloadStreams, Duration.fromMinutes(60));

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
        assertThat(stats.totalDurationAsMilli(), equalTo(Duration.fromMilli(expectedWorkloadDurationAsMilli)));
        assertThat(stats.firstStartTimeAsMilli(), equalTo(operation1StartTime));
        assertThat(stats.lastStartTimeAsMilli(), equalTo(operation1StartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));
        assertThat(stats.firstStartTimesAsMilliByOperationType().get(TimedNamedOperation1.class), equalTo(operation1StartTime));
        assertThat(stats.firstStartTimesAsMilliByOperationType().get(TimedNamedOperation2.class), equalTo(operation2StartTime));
        assertThat(stats.firstStartTimesAsMilliByOperationType().get(TimedNamedOperation3.class), equalTo(operation3StartTime));
        assertThat(stats.lastStartTimesAsMilliByOperationType().get(TimedNamedOperation1.class), equalTo(operation1StartTime.plus(Duration.fromMilli(expectedWorkloadOperation1DurationAsMilli))));
        assertThat(stats.lastStartTimesAsMilliByOperationType().get(TimedNamedOperation2.class), equalTo(operation2StartTime.plus(Duration.fromMilli(expectedWorkloadOperation2DurationAsMilli))));
        assertThat(stats.lastStartTimesAsMilliByOperationType().get(TimedNamedOperation3.class), equalTo(operation3StartTime.plus(Duration.fromMilli(expectedWorkloadOperation3DurationAsMilli))));

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

        ContinuousMetricSnapshot interleavesForDependencyOperations = stats.interleavesForDependencyOperations().snapshot();
        assertThat(Duration.fromMilli(Math.round(interleavesForDependencyOperations.mean())), is(operation3Interleave));
//        ContinuousMetricSnapshot interleavesForDependentOperations = stats.interleavesForDependentOperations().snapshot();
//        assertThat(Duration.fromMilli(Math.round(interleavesForDependentOperations.mean())), is());

        Set<Class> dependencyOperationTypes = stats.dependencyOperationTypes();
        assertThat(dependencyOperationTypes, equalTo((Set) Sets.<Class>newHashSet(TimedNamedOperation3.class)));
        Set<Class> dependentOperationTypes = stats.dependentOperationTypes();
        assertThat(dependentOperationTypes, equalTo((Set) Sets.<Class>newHashSet(TimedNamedOperation2.class, TimedNamedOperation3.class)));

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

        assertThat(stats.lowestDependencyDurationAsMilliByOperationType().get(TimedNamedOperation1.class), is(Duration.fromMilli(100)));
        assertThat(stats.lowestDependencyDurationAsMilliByOperationType().get(TimedNamedOperation2.class), is(Duration.fromMilli(100)));
        assertThat(stats.lowestDependencyDurationAsMilliByOperationType().get(TimedNamedOperation3.class), is(Duration.fromMilli(10)));

        System.out.println(stats.toString());
    }
}
