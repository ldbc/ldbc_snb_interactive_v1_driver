package com.ldbc.driver.statistics;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.MapUtils;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

public class WorkloadStatistics
{
    private final Map<Class,Long> firstStartTimesAsMilliByOperationType;
    private final Map<Class,Long> lastStartTimesAsMilliByOperationType;
    private final Histogram<Class,Long> operationMixHistogram;
    private final ContinuousMetricManager operationInterleaves;
    private final Map<Class,ContinuousMetricManager> operationInterleavesByOperationType;
    private final Set<Class> dependencyOperationTypes;
    private final Set<Class> dependentOperationTypes;
    private final Map<Class,Long> lowestDependencyDurationAsMilliByOperationType;

    public WorkloadStatistics( Map<Class,Long> firstStartTimesAsMilliByOperationType,
            Map<Class,Long> lastStartTimesAsMilliByOperationType,
            Histogram<Class,Long> operationMixHistogram,
            ContinuousMetricManager operationInterleaves,
            Map<Class,ContinuousMetricManager> operationInterleavesByOperationType,
            Set<Class> dependencyOperationTypes,
            Set<Class> dependentOperationTypes,
            Map<Class,Long> lowestDependencyDurationAsMilliByOperationType )
    {
        this.firstStartTimesAsMilliByOperationType = firstStartTimesAsMilliByOperationType;
        this.lastStartTimesAsMilliByOperationType = lastStartTimesAsMilliByOperationType;
        this.operationMixHistogram = operationMixHistogram;
        this.operationInterleaves = operationInterleaves;
        this.operationInterleavesByOperationType = operationInterleavesByOperationType;
        this.dependencyOperationTypes = dependencyOperationTypes;
        this.dependentOperationTypes = dependentOperationTypes;
        this.lowestDependencyDurationAsMilliByOperationType = lowestDependencyDurationAsMilliByOperationType;
    }

    public long totalCount()
    {
        long count = 0;
        for ( Map.Entry<Class,ContinuousMetricManager> operationInterleaveForOperationType :
                operationInterleavesByOperationType
                .entrySet() )
        {
            count += operationInterleaveForOperationType.getValue().snapshot().count();
            // because interleaves are the durations BETWEEN operation occurrences they will be off by one
            // if there are no occurrences there will be no start time for the operation type
            // if there ARE occurrences, we should increment count by 1
            if ( firstStartTimesAsMilliByOperationType.containsKey( operationInterleaveForOperationType.getKey() ) )
            { count += 1; }
        }
        return count;
    }

    public long totalDurationAsMilli()
    {
        long firstStartTimeAsMilli = firstStartTimeAsMilli();
        if ( -1 == firstStartTimeAsMilli )
        { return -1; }
        long lastStartTimeAsMilli = lastStartTimeAsMilli();
        if ( -1 == lastStartTimeAsMilli )
        { return -1; }
        return lastStartTimeAsMilli - firstStartTimeAsMilli;
    }

    public int operationTypeCount()
    {
        return Math.max( firstStartTimesAsMilliByOperationType().size(), operationMix().getBucketCount() );
    }

    public long firstStartTimeAsMilli()
    {
        long firstStartTime = -1;
        for ( Map.Entry<Class,Long> firstStartTimeForOperationType : firstStartTimesAsMilliByOperationType.entrySet() )
        {
            if ( -1 == firstStartTime || firstStartTimeForOperationType.getValue() < firstStartTime )
            { firstStartTime = firstStartTimeForOperationType.getValue(); }
        }
        return firstStartTime;
    }

    public long lastStartTimeAsMilli()
    {
        long lastStartTimeAsMilli = -1;
        for ( Map.Entry<Class,Long> lastStartTimeForOperationType : lastStartTimesAsMilliByOperationType.entrySet() )
        {
            if ( -1 == lastStartTimeAsMilli || lastStartTimeForOperationType.getValue() > lastStartTimeAsMilli )
            {
                lastStartTimeAsMilli = lastStartTimeForOperationType.getValue();
            }
        }
        return lastStartTimeAsMilli;
    }

    public Map<Class,Long> firstStartTimesAsMilliByOperationType()
    {
        return firstStartTimesAsMilliByOperationType;
    }

    public Map<Class,Long> lastStartTimesAsMilliByOperationType()
    {
        return lastStartTimesAsMilliByOperationType;
    }

    public Histogram<Class,Long> operationMix()
    {
        return operationMixHistogram;
    }

    public ContinuousMetricManager operationInterleaves()
    {
        return operationInterleaves;
    }

    public Map<Class,ContinuousMetricManager> operationInterleavesByOperationType()
    {
        return operationInterleavesByOperationType;
    }

    public Set<Class> dependencyOperationTypes()
    {
        return dependencyOperationTypes;
    }

    public Set<Class> dependentOperationTypes()
    {
        return dependentOperationTypes;
    }

    public Map<Class,Long> lowestDependencyDurationAsMilliByOperationType()
    {
        return lowestDependencyDurationAsMilliByOperationType;
    }

    @Override
    public String toString()
    {
        TemporalUtil temporalUtil = new TemporalUtil();
        DecimalFormat integralFormat = new DecimalFormat( "###,###,###,###,###" );
        DecimalFormat floatFormat = new DecimalFormat( "###,###,###,###,#00.00" );
        int padRightDistance = 40;
        StringBuilder sb = new StringBuilder();
        sb.append( "********************************************************\n" );
        sb.append( "************ Calculated Workload Statistics ************\n" );
        sb.append( "********************************************************\n" );
        sb.append( "  ------------------------------------------------------\n" );
        sb.append( "  GENERAL\n" );
        sb.append( "  ------------------------------------------------------\n" );
        double opsPerS = totalCount() / (double) totalDurationAsMilli() * 1000;
        sb.append( format( "%1$-" + padRightDistance + "s", "     Operation Count:" ) )
                .append( integralFormat.format( totalCount() ) ).append( "\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "     Total Duration:" ) )
                .append( temporalUtil.milliDurationToString( totalDurationAsMilli() ) ).append( "\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "     Throughput:" ) )
                .append( floatFormat.format( opsPerS ) ).append( " (op/s)\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "     Unique Operation Types:" ) )
                .append( integralFormat.format( operationTypeCount() ) ).append( "\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "     Time Span:" ) )
                .append( temporalUtil.milliTimeToDateTimeString( firstStartTimeAsMilli() ) ).append( " ===> " )
                .append( temporalUtil.milliTimeToDateTimeString( lastStartTimeAsMilli() ) ).append( "\n" );
        sb.append( "     Operation Mix:\n" );

        List<Map.Entry<Bucket<Class>,Long>> absoluteOperationMixEntryList =
                Lists.newArrayList( MapUtils.sortedEntries( operationMix().getAllBuckets() ) );
        List<Map.Entry<Bucket<Class>,Double>> percentageOperationMixEntryList =
                Lists.newArrayList( MapUtils.sortedEntries( operationMix().toPercentageValues().getAllBuckets() ) );
        for ( int i = 0; i < absoluteOperationMixEntryList.size(); i++ )
        {
            Map.Entry<Bucket<Class>,Long> absoluteOperationMixEntry = absoluteOperationMixEntryList.get( i );
            Map.Entry<Bucket<Class>,Double> percentageOperationMixEntry = percentageOperationMixEntryList.get( i );
            Bucket.DiscreteBucket<Class> bucket = (Bucket.DiscreteBucket<Class>) absoluteOperationMixEntry.getKey();
            Class<Operation> operationType = bucket.getId();
            long absoluteOperationCount = absoluteOperationMixEntry.getValue();
            double percentageOperationCount = percentageOperationMixEntry.getValue();
            sb.append( format( "%1$-" + padRightDistance + "s", "        " + operationType.getSimpleName() + ":" ) );
            sb.append( format( "%1$-" + 20 + "s", integralFormat.format( absoluteOperationCount ) ) );
            sb.append( floatFormat.format( percentageOperationCount * 100 ) ).append( " %" ).append( "\n" );

        }

        sb.append( "     Operation By Dependency Mode:\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "        All Operations:" ) )
                .append( toSortedClassNames( firstStartTimesAsMilliByOperationType.keySet() ) ).append( "\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "        Dependency Operations:" ) )
                .append( toSortedClassNames( dependencyOperationTypes ) ).append( "\n" );
        sb.append( format( "%1$-" + padRightDistance + "s", "        Dependent Operations:" ) )
                .append( toSortedClassNames( dependentOperationTypes ) ).append( "\n" );
        sb.append( "  ------------------------------------------------------\n" );
        sb.append( "  INTERLEAVES\n" );
        sb.append( "  ------------------------------------------------------\n" );
        ContinuousMetricSnapshot interleavesSnapshot = operationInterleaves().snapshot();
        sb.append( format( "%1$-" + padRightDistance + "s", "        All Operations:" ) ).
                append( "min = " ).append( temporalUtil.milliDurationToString( interleavesSnapshot.min() ) )
                .append( " / " ).
                append( "mean = " )
                .append( temporalUtil.milliDurationToString( Math.round( interleavesSnapshot.mean() ) ) )
                .append( " / " ).
                append( "max = " ).append( temporalUtil.milliDurationToString( interleavesSnapshot.max() ) )
                .append( "\n" );
        sb.append( "  ------------------------------------------------------\n" );
        sb.append( "  BY OPERATION TYPE\n" );
        sb.append( "  ------------------------------------------------------\n" );
        for ( Map.Entry<Class,Long> lowestDependencyDurationAsMilliForOperationType : MapUtils
                .sortedEntrySet( lowestDependencyDurationAsMilliByOperationType() ) )
        {
            Class<Operation> operationType = lowestDependencyDurationAsMilliForOperationType.getKey();
            long firstStartAsMilliTypeForOperationType = firstStartTimesAsMilliByOperationType().get( operationType );
            long lastStartAsMilliTypeForOperationType = lastStartTimesAsMilliByOperationType().get( operationType );
            sb.append( format( "%1$-" + padRightDistance + "s", "     " + operationType.getSimpleName() + ":" ) ).
                    append( "Min Dependency Duration(" ).append(
                    temporalUtil.milliDurationToString( lowestDependencyDurationAsMilliForOperationType.getValue() ) )
                    .append( ") " );
            if ( operationInterleavesByOperationType().containsKey( operationType ) )
            {
                ContinuousMetricSnapshot interleavesForOperationTypeSnapshot =
                        operationInterleavesByOperationType().get( operationType ).snapshot();
                sb.
                        append( "Time Span(" ).
                        append( temporalUtil.milliTimeToTimeString( firstStartAsMilliTypeForOperationType ) )
                        .append( ", " )
                        .append( temporalUtil.milliTimeToTimeString( lastStartAsMilliTypeForOperationType ) )
                        .append( ") " ).
                        append( "Interleave(" ).
                        append( "min = " )
                        .append( temporalUtil.milliDurationToString( interleavesForOperationTypeSnapshot.min() ) )
                        .append( " / " ).
                        append( "mean = " ).append(
                        temporalUtil.milliDurationToString( Math.round( interleavesForOperationTypeSnapshot.mean() ) ) )
                        .append( " / " ).
                        append( "max = " )
                        .append( temporalUtil.milliDurationToString( interleavesForOperationTypeSnapshot.max() ) )
                        .append( ")" );
            }
            sb.append( "\n" );
        }
        sb.append( "********************************************************" );
        return sb.toString();
    }

    private List<String> toSortedClassNames( Iterable<Class> classes )
    {
        List<String> classNames = Lists.newArrayList(
                Iterables.transform(
                        classes,
                        new Function<Class,String>()
                        {
                            @Override
                            public String apply( Class aClass )
                            {
                                return aClass.getSimpleName();
                            }
                        } )
        );
        Collections.sort( classNames );
        return classNames;
    }
}