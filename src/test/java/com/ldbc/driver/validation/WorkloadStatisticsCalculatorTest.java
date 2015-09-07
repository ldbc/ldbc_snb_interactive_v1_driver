package com.ldbc.driver.validation;

import com.google.common.collect.Sets;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.statistics.WorkloadStatistics;
import com.ldbc.driver.statistics.WorkloadStatisticsCalculator;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation3;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation3Factory;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class WorkloadStatisticsCalculatorTest
{
    private GeneratorFactory gf;

    @Before
    public void init()
    {
        gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithSingleOperationType()
            throws MetricsCollectionException
    {
        // Given
        long workloadStartTime = 0l;
        long operationCount = 1000;
        long operationInterleave = 100l;

        Iterator<Operation> operations = gf.limit(
                new TimedNamedOperation1Factory(
                        gf.incrementing( workloadStartTime, operationInterleave ),
                        gf.incrementing( workloadStartTime - operationInterleave, operationInterleave ),
                        gf.constant( "name1" )
                ),
                operationCount );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        workloadStreams.setAsynchronousStream(
                Sets.<Class<? extends Operation>>newHashSet(),
                Sets.<Class<? extends Operation>>newHashSet(),
                Collections.<Operation>emptyIterator(),
                operations,
                null
        );

        // When

        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate( workloadStreams, TimeUnit.MINUTES.toMillis( 60 ) );

        // Then

        // expected values
        long expectedWorkloadDurationAsMilli = (operationCount - 1) * operationInterleave;

        assertThat( stats.totalCount(), is( operationCount ) );
        assertThat( stats.operationTypeCount(), is( 1 ) );
        assertThat( stats.totalDurationAsMilli(), equalTo( expectedWorkloadDurationAsMilli ) );
        assertThat( stats.firstStartTimeAsMilli(), equalTo( workloadStartTime ) );
        assertThat( stats.lastStartTimeAsMilli(), equalTo( workloadStartTime + expectedWorkloadDurationAsMilli ) );
        assertThat( stats.firstStartTimesAsMilliByOperationType().get( TimedNamedOperation1.class ),
                equalTo( workloadStartTime ) );
        assertThat( stats.lastStartTimesAsMilliByOperationType().get( TimedNamedOperation1.class ),
                equalTo( workloadStartTime + expectedWorkloadDurationAsMilli ) );

        double tolerance = 0.01d;
        Histogram<Class,Double> expectedOperationMix = new Histogram<>( 0d );
        expectedOperationMix.addBucket( Bucket.DiscreteBucket.create( (Class) TimedNamedOperation1.class ), 1d );
        assertThat(
                format( "Distributions should be within tolerance: %s\n%s\n%s",
                        tolerance,
                        stats.operationMix().toPercentageValues().toPrettyString(),
                        expectedOperationMix.toPercentageValues().toPrettyString() ),
                Histogram.equalsWithinTolerance(
                        stats.operationMix().toPercentageValues(),
                        expectedOperationMix.toPercentageValues(),
                        tolerance ),
                is( true ) );

        ContinuousMetricSnapshot operationInterleaves = stats.operationInterleaves().snapshot();
        assertThat( operationInterleaves.min(), equalTo( operationInterleave ) );
        assertThat( operationInterleaves.mean(), equalTo( (double) operationInterleave ) );
        assertThat( operationInterleaves.percentile95(), equalTo( operationInterleave ) );
        assertThat( operationInterleaves.max(), equalTo( operationInterleave ) );
        assertThat( operationInterleaves.count(), equalTo( operationCount - 1 ) );

        ContinuousMetricSnapshot operation1Interleaves =
                stats.operationInterleavesByOperationType().get( TimedNamedOperation1.class ).snapshot();
        assertThat( operation1Interleaves.min(), is( operationInterleave ) );
        assertThat( operation1Interleaves.max(), is( operationInterleave ) );
        assertThat( operation1Interleaves.count(), is( operationCount - 1 ) );
        assertThat( operation1Interleaves.mean(), is( (double) operationInterleave ) );

        assertThat( stats.operationInterleavesByOperationType().get( TimedNamedOperation2.class ), is( nullValue() ) );

        assertThat( stats.operationInterleavesByOperationType().get( TimedNamedOperation3.class ), is( nullValue() ) );

        assertThat( stats.dependencyOperationTypes(), equalTo( (Set) new HashSet<Class>() ) );
        assertThat( stats.dependentOperationTypes(), equalTo( (Set) new HashSet<Class>() ) );

        System.out.println( stats.toString() );
    }

    @Test
    public void shouldReturnCorrectWorkloadStatisticsForWorkloadsWithMultipleOperationTypes()
            throws MetricsCollectionException
    {
        // Given
        long operation1Count = 1000;
        long operation1StartTime = 100l;
        long operation1Interleave = 100l; //100,100

        long operation2Count = 100;
        long operation2StartTime = 100l;
        long operation2Interleave = 1000l; // 100,100

        long operation3Count = 10;
        long operation3StartTime = 100l;
        long operation3Interleave = 10000l; // 100,100

        Iterator<Operation> operation1Stream = gf.limit(
                new TimedNamedOperation1Factory(
                        gf.incrementing( operation1StartTime, operation1Interleave ),
                        gf.incrementing( 0l, 0l ),
                        gf.constant( "name1" )
                ),
                operation1Count );
        Iterator<Operation> operation2Stream = gf.limit(
                new TimedNamedOperation2Factory(
                        gf.incrementing( operation2StartTime, operation2Interleave ),
                        gf.incrementing( 0l, operation2Interleave ),
                        gf.constant( "name2" )
                ),
                operation2Count );
        Iterator<Operation> operation3Stream = gf.limit(
                new TimedNamedOperation3Factory(
                        gf.incrementing( operation3StartTime, operation3Interleave ),
                        gf.incrementing( operation3StartTime - 10l, operation3Interleave ),
                        gf.constant( "name3" )
                ),
                operation3Count );

        WorkloadStreams workloadStreams = new WorkloadStreams();
        Set<Class<? extends Operation>> dependentOperations = Sets.<Class<? extends Operation>>newHashSet(
                TimedNamedOperation2.class,
                TimedNamedOperation3.class
        );
        workloadStreams.setAsynchronousStream(
                dependentOperations,
                Sets.<Class<? extends Operation>>newHashSet( TimedNamedOperation3.class ),
                operation3Stream,
                gf.mergeSortOperationsByTimeStamp( operation1Stream, operation2Stream ),
                null
        );

        // When

        WorkloadStatisticsCalculator calculator = new WorkloadStatisticsCalculator();
        WorkloadStatistics stats = calculator.calculate( workloadStreams, TimeUnit.MINUTES.toMillis( 60 ) );

        // Then

        // expected values
        long expectedOperationCount = operation1Count + operation2Count + operation3Count;

        long expectedWorkloadOperation1DurationAsMilli = (operation1Count - 1) * operation1Interleave;
        long expectedWorkloadOperation2DurationAsMilli = (operation2Count - 1) * operation2Interleave;
        long expectedWorkloadOperation3DurationAsMilli = (operation3Count - 1) * operation3Interleave;
        long expectedWorkloadDurationAsMilli =
                Math.max( Math.max( expectedWorkloadOperation1DurationAsMilli,
                        expectedWorkloadOperation2DurationAsMilli ), expectedWorkloadOperation3DurationAsMilli );

        assertThat( stats.totalCount(), is( operation1Count + operation2Count + operation3Count ) );
        assertThat( stats.operationTypeCount(), is( 3 ) );
        assertThat( stats.totalDurationAsMilli(), equalTo( expectedWorkloadDurationAsMilli ) );
        assertThat( stats.firstStartTimeAsMilli(), equalTo( operation1StartTime ) );
        assertThat( stats.lastStartTimeAsMilli(), equalTo( operation1StartTime + expectedWorkloadDurationAsMilli ) );
        assertThat( stats.firstStartTimesAsMilliByOperationType().get( TimedNamedOperation1.class ),
                equalTo( operation1StartTime ) );
        assertThat( stats.firstStartTimesAsMilliByOperationType().get( TimedNamedOperation2.class ),
                equalTo( operation2StartTime ) );
        assertThat( stats.firstStartTimesAsMilliByOperationType().get( TimedNamedOperation3.class ),
                equalTo( operation3StartTime ) );
        assertThat( stats.lastStartTimesAsMilliByOperationType().get( TimedNamedOperation1.class ),
                equalTo( operation1StartTime + expectedWorkloadOperation1DurationAsMilli ) );
        assertThat( stats.lastStartTimesAsMilliByOperationType().get( TimedNamedOperation2.class ),
                equalTo( operation2StartTime + expectedWorkloadOperation2DurationAsMilli ) );
        assertThat( stats.lastStartTimesAsMilliByOperationType().get( TimedNamedOperation3.class ),
                equalTo( operation3StartTime + expectedWorkloadOperation3DurationAsMilli ) );

        double tolerance = 0.01d;
        Histogram<Class,Double> expectedOperationMix = new Histogram<>( 0d );
        expectedOperationMix.addBucket( Bucket.DiscreteBucket.create( (Class) TimedNamedOperation1.class ),
                operation1Count / (double) expectedOperationCount );
        expectedOperationMix.addBucket( Bucket.DiscreteBucket.create( (Class) TimedNamedOperation2.class ),
                operation2Count / (double) expectedOperationCount );
        expectedOperationMix.addBucket( Bucket.DiscreteBucket.create( (Class) TimedNamedOperation3.class ),
                operation3Count / (double) expectedOperationCount );
        assertThat(
                format( "Distributions should be within tolerance: %s\n%s\n%s",
                        tolerance,
                        stats.operationMix().toPercentageValues().toPrettyString(),
                        expectedOperationMix.toPercentageValues().toPrettyString() ),
                Histogram.equalsWithinTolerance(
                        stats.operationMix().toPercentageValues(),
                        expectedOperationMix.toPercentageValues(),
                        tolerance ),
                is( true ) );

        Set<Class> dependencyOperationTypes = stats.dependencyOperationTypes();
        assertThat( dependencyOperationTypes, equalTo( (Set) Sets.<Class>newHashSet( TimedNamedOperation3.class ) ) );
        Set<Class> dependentOperationTypes = stats.dependentOperationTypes();
        assertThat( dependentOperationTypes,
                equalTo( (Set) Sets.<Class>newHashSet( TimedNamedOperation2.class, TimedNamedOperation3.class ) ) );

        ContinuousMetricSnapshot operation1Interleaves =
                stats.operationInterleavesByOperationType().get( TimedNamedOperation1.class ).snapshot();
        assertThat( operation1Interleaves.min(), is( operation1Interleave ) );
        assertThat( operation1Interleaves.max(), is( operation1Interleave ) );
        assertThat( operation1Interleaves.count(), is( operation1Count - 1 ) );
        assertThat( operation1Interleaves.mean(), is( (double) operation1Interleave ) );

        ContinuousMetricSnapshot operation2Interleaves =
                stats.operationInterleavesByOperationType().get( TimedNamedOperation2.class ).snapshot();
        assertThat( operation2Interleaves.min(), is( operation2Interleave ) );
        assertThat( operation2Interleaves.max(), is( operation2Interleave ) );
        assertThat( operation2Interleaves.count(), is( operation2Count - 1 ) );
        assertThat( operation2Interleaves.mean(), is( (double) operation2Interleave ) );

        ContinuousMetricSnapshot operation3Interleaves =
                stats.operationInterleavesByOperationType().get( TimedNamedOperation3.class ).snapshot();
        assertThat( operation3Interleaves.min(), is( operation3Interleave ) );
        assertThat( operation3Interleaves.max(), is( operation3Interleave ) );
        assertThat( operation3Interleaves.count(), is( operation3Count - 1 ) );
        assertThat( operation3Interleaves.mean(), is( (double) operation3Interleave ) );

        assertThat( stats.lowestDependencyDurationAsMilliByOperationType().get( TimedNamedOperation1.class ),
                is( 100l ) );
        assertThat( stats.lowestDependencyDurationAsMilliByOperationType().get( TimedNamedOperation2.class ),
                is( 100l ) );
        assertThat( stats.lowestDependencyDurationAsMilliByOperationType().get( TimedNamedOperation3.class ),
                is( 10l ) );

        System.out.println( stats.toString() );
    }
}
