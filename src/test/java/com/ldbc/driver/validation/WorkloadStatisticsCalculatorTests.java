package com.ldbc.driver.validation;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.OperationClassification.GctMode;
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
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WorkloadStatisticsCalculatorTests {
    private TimeSource TIME_SOURCE = new SystemTimeSource();
    private GeneratorFactory generators;

    @Before
    public void init() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithSingleOperationType() throws MetricsCollectionException {
        // Given
        Time workloadStartTime = TIME_SOURCE.now();
        long operationCount = 1000;
        Duration operationInterleave = Duration.fromMilli(100);

        Iterator<Operation<?>> operationStreamWithoutTime = generators.limit(new Operation1Iterator(), operationCount);
        Iterator<Time> operationStartTimes = generators.constantIncrementTime(workloadStartTime, operationInterleave);
        Iterator<Operation<?>> operationStream = generators.startTimeAssigning(operationStartTimes, operationStreamWithoutTime);


        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(Operation1.class, new OperationClassification(null, GctMode.NONE));

        // When
        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate(operationStream, operationClassifications, WorkloadValidator.DEFAULT_MAX_EXPECTED_INTERLEAVE);

        // Then

        // expected values
        long expectedWorkloadDurationAsMilli = (operationCount - 1) * operationInterleave.asMilli();

        assertThat(stats.totalCount(), is(operationCount));
        assertThat(stats.operationTypeCount(), is(1));
        assertThat(stats.totalDuration(), equalTo(Duration.fromMilli(expectedWorkloadDurationAsMilli)));
        assertThat(stats.firstStartTime(), equalTo(workloadStartTime));
        assertThat(stats.lastStartTime(), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));
        assertThat(stats.firstStartTimesByOperationType().get(Operation1.class), equalTo(workloadStartTime));
        assertThat(stats.lastStartTimesByOperationType().get(Operation1.class), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));

        double tolerance = 0.01d;
        Histogram<Class, Double> expectedOperationMix = new Histogram<>(0d);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) Operation1.class), 1d);
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

        ContinuousMetricSnapshot gctNoneInterleaves = stats.operationInterleavesByGctMode().get(GctMode.NONE).snapshot();
        assertThat(gctNoneInterleaves.min(), is(operationInterleave.asMilli()));
        assertThat(gctNoneInterleaves.max(), is(operationInterleave.asMilli()));
        assertThat(gctNoneInterleaves.count(), is(operationCount - 1));
        assertThat(gctNoneInterleaves.mean(), is((double) operationInterleave.asMilli()));

        assertThat(stats.operationInterleavesByGctMode().get(GctMode.READ), is(nullValue()));

        assertThat(stats.operationInterleavesByGctMode().get(GctMode.READ_WRITE), is(nullValue()));

        ContinuousMetricSnapshot operation1Interleaves = stats.operationInterleavesByOperationType().get(Operation1.class).snapshot();
        assertThat(operation1Interleaves.min(), is(operationInterleave.asMilli()));
        assertThat(operation1Interleaves.max(), is(operationInterleave.asMilli()));
        assertThat(operation1Interleaves.count(), is(operationCount - 1));
        assertThat(operation1Interleaves.mean(), is((double) operationInterleave.asMilli()));

        assertThat(stats.operationInterleavesByOperationType().get(Operation2.class), is(nullValue()));

        assertThat(stats.operationInterleavesByOperationType().get(Operation3.class), is(nullValue()));

        assertThat(stats.operationsByGctMode().get(GctMode.NONE), equalTo((Set) Sets.newHashSet(Operation1.class)));
        assertThat(stats.operationsByGctMode().get(GctMode.READ), is(nullValue()));
        assertThat(stats.operationsByGctMode().get(GctMode.READ_WRITE), is(nullValue()));

        System.out.println(stats.toString());
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithMultipleOperationTypes() throws MetricsCollectionException {
        // Given
        Time workloadStartTime = TIME_SOURCE.now();
        long operation1Count = 1000;
        Duration operation1Interleave = Duration.fromMilli(100);
        long operation2Count = 100;
        Duration operation2Interleave = Duration.fromMilli(1000);
        long operation3Count = 10;
        Duration operation3Interleave = Duration.fromMilli(10000);

        Iterator<Operation<?>> operation1StreamWithoutTime = generators.limit(new Operation1Iterator(), operation1Count);
        Iterator<Time> operation1StartTimes = generators.constantIncrementTime(workloadStartTime, operation1Interleave);
        Iterator<Operation<?>> operation1Stream = generators.startTimeAssigning(operation1StartTimes, operation1StreamWithoutTime);
        Iterator<Operation<?>> operation2StreamWithoutTime = generators.limit(new Operation2Iterator(), operation2Count);
        Iterator<Time> operation2StartTimes = generators.constantIncrementTime(workloadStartTime, operation2Interleave);
        Iterator<Operation<?>> operation2Stream = generators.startTimeAssigning(operation2StartTimes, operation2StreamWithoutTime);
        Iterator<Operation<?>> operation3StreamWithoutTime = generators.limit(new Operation3Iterator(), operation3Count);
        Iterator<Time> operation3StartTimes = generators.constantIncrementTime(workloadStartTime, operation3Interleave);
        Iterator<Operation<?>> operation3Stream = generators.startTimeAssigning(operation3StartTimes, operation3StreamWithoutTime);

        Iterator<Operation<?>> operationStream = generators.mergeSortOperationsByStartTime(operation1Stream, operation2Stream, operation3Stream);

        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(Operation1.class, new OperationClassification(null, GctMode.NONE));
        operationClassifications.put(Operation2.class, new OperationClassification(null, GctMode.READ));
        operationClassifications.put(Operation3.class, new OperationClassification(null, GctMode.READ_WRITE));

        // When
        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate(operationStream, operationClassifications, WorkloadValidator.DEFAULT_MAX_EXPECTED_INTERLEAVE);

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
        assertThat(stats.firstStartTime(), equalTo(workloadStartTime));
        assertThat(stats.lastStartTime(), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadDurationAsMilli))));
        assertThat(stats.firstStartTimesByOperationType().get(Operation1.class), equalTo(workloadStartTime));
        assertThat(stats.firstStartTimesByOperationType().get(Operation2.class), equalTo(workloadStartTime));
        assertThat(stats.firstStartTimesByOperationType().get(Operation3.class), equalTo(workloadStartTime));
        assertThat(stats.lastStartTimesByOperationType().get(Operation1.class), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadOperation1DurationAsMilli))));
        assertThat(stats.lastStartTimesByOperationType().get(Operation2.class), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadOperation2DurationAsMilli))));
        assertThat(stats.lastStartTimesByOperationType().get(Operation3.class), equalTo(workloadStartTime.plus(Duration.fromMilli(expectedWorkloadOperation3DurationAsMilli))));

        double tolerance = 0.01d;
        Histogram<Class, Double> expectedOperationMix = new Histogram<>(0d);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) Operation1.class), operation1Count / (double) expectedOperationCount);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) Operation2.class), operation2Count / (double) expectedOperationCount);
        expectedOperationMix.addBucket(Bucket.DiscreteBucket.create((Class) Operation3.class), operation3Count / (double) expectedOperationCount);
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

        ContinuousMetricSnapshot gctNoneInterleaves = stats.operationInterleavesByGctMode().get(GctMode.NONE).snapshot();
        assertThat(gctNoneInterleaves.min(), is(operation1Interleave.asMilli()));
        assertThat(gctNoneInterleaves.max(), is(operation1Interleave.asMilli()));
        assertThat(gctNoneInterleaves.count(), is(operation1Count - 1));
        assertThat(gctNoneInterleaves.mean(), is((double) operation1Interleave.asMilli()));

        ContinuousMetricSnapshot gctReadInterleaves = stats.operationInterleavesByGctMode().get(GctMode.READ).snapshot();
        assertThat(gctReadInterleaves.min(), is(operation2Interleave.asMilli()));
        assertThat(gctReadInterleaves.max(), is(operation2Interleave.asMilli()));
        assertThat(gctReadInterleaves.count(), is(operation2Count - 1));
        assertThat(gctReadInterleaves.mean(), is((double) operation2Interleave.asMilli()));

        ContinuousMetricSnapshot gctReadWriteInterleaves = stats.operationInterleavesByGctMode().get(GctMode.READ_WRITE).snapshot();
        assertThat(gctReadWriteInterleaves.min(), is(operation3Interleave.asMilli()));
        assertThat(gctReadWriteInterleaves.max(), is(operation3Interleave.asMilli()));
        assertThat(gctReadWriteInterleaves.count(), is(operation3Count - 1));
        assertThat(gctReadWriteInterleaves.mean(), is((double) operation3Interleave.asMilli()));

        ContinuousMetricSnapshot operation1Interleaves = stats.operationInterleavesByOperationType().get(Operation1.class).snapshot();
        assertThat(operation1Interleaves.min(), is(operation1Interleave.asMilli()));
        assertThat(operation1Interleaves.max(), is(operation1Interleave.asMilli()));
        assertThat(operation1Interleaves.count(), is(operation1Count - 1));
        assertThat(operation1Interleaves.mean(), is((double) operation1Interleave.asMilli()));

        ContinuousMetricSnapshot operation2Interleaves = stats.operationInterleavesByOperationType().get(Operation2.class).snapshot();
        assertThat(operation2Interleaves.min(), is(operation2Interleave.asMilli()));
        assertThat(operation2Interleaves.max(), is(operation2Interleave.asMilli()));
        assertThat(operation2Interleaves.count(), is(operation2Count - 1));
        assertThat(operation2Interleaves.mean(), is((double) operation2Interleave.asMilli()));

        ContinuousMetricSnapshot operation3Interleaves = stats.operationInterleavesByOperationType().get(Operation3.class).snapshot();
        assertThat(operation3Interleaves.min(), is(operation3Interleave.asMilli()));
        assertThat(operation3Interleaves.max(), is(operation3Interleave.asMilli()));
        assertThat(operation3Interleaves.count(), is(operation3Count - 1));
        assertThat(operation3Interleaves.mean(), is((double) operation3Interleave.asMilli()));

        assertThat(stats.operationsByGctMode().get(GctMode.NONE), equalTo((Set) Sets.newHashSet(Operation1.class)));
        assertThat(stats.operationsByGctMode().get(GctMode.READ), equalTo((Set) Sets.newHashSet(Operation2.class)));
        assertThat(stats.operationsByGctMode().get(GctMode.READ_WRITE), equalTo((Set) Sets.newHashSet(Operation3.class)));

        System.out.println(stats.toString());
    }

    private static class Operation1Iterator implements Iterator<Operation<?>> {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Operation<?> next() {
            return new Operation1();
        }

        @Override
        public void remove() {
        }
    }

    private static class Operation2Iterator implements Iterator<Operation<?>> {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Operation<?> next() {
            return new Operation2();
        }

        @Override
        public void remove() {
        }
    }

    private static class Operation3Iterator implements Iterator<Operation<?>> {
        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Operation<?> next() {
            return new Operation3();
        }

        @Override
        public void remove() {
        }
    }

    private static class Operation1 extends Operation<Object> {
    }

    private static class Operation2 extends Operation<Object> {
    }

    private static class Operation3 extends Operation<Object> {
    }
}
