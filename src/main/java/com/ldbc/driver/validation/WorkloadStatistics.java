package com.ldbc.driver.validation;

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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WorkloadStatistics {
    private final Map<Class, Long> firstStartTimesAsMilliByOperationType;
    private final Map<Class, Long> lastStartTimesAsMilliByOperationType;
    private final Histogram<Class, Long> operationMixHistogram;
    private final ContinuousMetricManager operationInterleaves;
    private final ContinuousMetricManager interleavesForDependencyOperations;
    private final ContinuousMetricManager interleavesForDependentOperations;
    private final Map<Class, ContinuousMetricManager> operationInterleavesByOperationType;
    private final Set<Class> dependencyOperationTypes;
    private final Set<Class> dependentOperationTypes;
    private final Map<Class, Long> lowestDependencyDurationAsMilliByOperationType;

    public WorkloadStatistics(Map<Class, Long> firstStartTimesAsMilliByOperationType,
                              Map<Class, Long> lastStartTimesAsMilliByOperationType,
                              Histogram<Class, Long> operationMixHistogram,
                              ContinuousMetricManager operationInterleaves,
                              ContinuousMetricManager interleavesForDependencyOperations,
                              ContinuousMetricManager interleavesForDependentOperations,
                              Map<Class, ContinuousMetricManager> operationInterleavesByOperationType,
                              Set<Class> dependencyOperationTypes,
                              Set<Class> dependentOperationTypes,
                              Map<Class, Long> lowestDependencyDurationAsMilliByOperationType) {
        this.firstStartTimesAsMilliByOperationType = firstStartTimesAsMilliByOperationType;
        this.lastStartTimesAsMilliByOperationType = lastStartTimesAsMilliByOperationType;
        this.operationMixHistogram = operationMixHistogram;
        this.operationInterleaves = operationInterleaves;
        this.interleavesForDependencyOperations = interleavesForDependencyOperations;
        this.interleavesForDependentOperations = interleavesForDependentOperations;
        this.operationInterleavesByOperationType = operationInterleavesByOperationType;
        this.dependencyOperationTypes = dependencyOperationTypes;
        this.dependentOperationTypes = dependentOperationTypes;
        this.lowestDependencyDurationAsMilliByOperationType = lowestDependencyDurationAsMilliByOperationType;
    }

    public long totalCount() {
        long count = 0;
        for (Map.Entry<Class, ContinuousMetricManager> operationInterleaveForOperationType : operationInterleavesByOperationType.entrySet()) {
            count += operationInterleaveForOperationType.getValue().snapshot().count();
            // because interleaves are the durations BETWEEN operation occurrences they will be off by one
            // if there are no occurrences there will be no start time for the operation type
            // if there ARE occurrences, we should increment count by 1
            if (firstStartTimesAsMilliByOperationType.containsKey(operationInterleaveForOperationType.getKey()))
                count += 1;
        }
        return count;
    }

    public long totalDurationAsMilli() {
        long firstStartTimeAsMilli = firstStartTimeAsMilli();
        if (-1 == firstStartTimeAsMilli) return -1;
        long lastStartTimeAsMilli = lastStartTimeAsMilli();
        if (-1 == lastStartTimeAsMilli) return -1;
        return lastStartTimeAsMilli - firstStartTimeAsMilli;
    }

    public int operationTypeCount() {
        return Math.max(firstStartTimesAsMilliByOperationType().size(), operationMix().getBucketCount());
    }

    public long firstStartTimeAsMilli() {
        long firstStartTime = -1;
        for (Map.Entry<Class, Long> firstStartTimeForOperationType : firstStartTimesAsMilliByOperationType.entrySet()) {
            if (-1 == firstStartTime || firstStartTimeForOperationType.getValue() < firstStartTime)
                firstStartTime = firstStartTimeForOperationType.getValue();
        }
        return firstStartTime;
    }

    public long lastStartTimeAsMilli() {
        long lastStartTimeAsMilli = -1;
        for (Map.Entry<Class, Long> lastStartTimeForOperationType : lastStartTimesAsMilliByOperationType.entrySet()) {
            if (-1 == lastStartTimeAsMilli || lastStartTimeForOperationType.getValue() < lastStartTimeAsMilli)
                lastStartTimeAsMilli = lastStartTimeForOperationType.getValue();
        }
        return lastStartTimeAsMilli;
    }

    public Map<Class, Long> firstStartTimesAsMilliByOperationType() {
        return firstStartTimesAsMilliByOperationType;
    }

    public Map<Class, Long> lastStartTimesAsMilliByOperationType() {
        return lastStartTimesAsMilliByOperationType;
    }

    public Histogram<Class, Long> operationMix() {
        return operationMixHistogram;
    }

    public ContinuousMetricManager operationInterleaves() {
        return operationInterleaves;
    }

    public ContinuousMetricManager interleavesForDependencyOperations() {
        return interleavesForDependencyOperations;
    }

    public ContinuousMetricManager interleavesForDependentOperations() {
        return interleavesForDependentOperations;
    }

    public Map<Class, ContinuousMetricManager> operationInterleavesByOperationType() {
        return operationInterleavesByOperationType;
    }

    public Set<Class> dependencyOperationTypes() {
        return dependencyOperationTypes;
    }

    public Set<Class> dependentOperationTypes() {
        return dependentOperationTypes;
    }

    public Map<Class, Long> lowestDependencyDurationAsMilliByOperationType() {
        return lowestDependencyDurationAsMilliByOperationType;
    }

    @Override
    public String toString() {
        TemporalUtil temporalUtil = new TemporalUtil();
        int padRightDistance = 40;
        StringBuilder sb = new StringBuilder();
        sb.append("********************************************************\n");
        sb.append("************ Calculated Workload Statistics ************\n");
        sb.append("********************************************************\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("  GENERAL\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Operation Count:")).append(totalCount()).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Unique Operation Types:")).append(operationTypeCount()).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Total Duration:")).append(totalDurationAsMilli()).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Time Span:")).append(firstStartTimeAsMilli()).append(", ").append(lastStartTimeAsMilli()).append("\n");
        sb.append("     Operation Mix:\n");
        for (Map.Entry<Bucket<Class>, Long> operationMixForOperationType : MapUtils.sortedEntries(operationMix().getAllBuckets())) {
            Bucket.DiscreteBucket<Class> bucket = (Bucket.DiscreteBucket<Class>) operationMixForOperationType.getKey();
            Class<Operation<?>> operationType = bucket.getId();
            long operationCount = operationMixForOperationType.getValue();
            sb.append(String.format("%1$-" + padRightDistance + "s", "        " + operationType.getSimpleName() + ":")).append(operationCount).append("\n");
        }
        sb.append("     Operation By Dependency Mode:\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "        All Operations:")).append(toSortedClassNames(firstStartTimesAsMilliByOperationType.keySet())).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependency Operations:")).append(toSortedClassNames(dependencyOperationTypes)).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependent Operations:")).append(toSortedClassNames(dependentOperationTypes)).append("\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("  INTERLEAVES\n");
        sb.append("  ------------------------------------------------------\n");
        ContinuousMetricSnapshot interleavesSnapshot = operationInterleaves().snapshot();
        sb.append(String.format("%1$-" + padRightDistance + "s", "        All Operations:")).
                append("min = ").append(temporalUtil.milliDurationToString(interleavesSnapshot.min())).append(" / ").
                append("mean = ").append(temporalUtil.milliDurationToString(Math.round(interleavesSnapshot.mean()))).append(" / ").
                append("max = ").append(temporalUtil.milliDurationToString(interleavesSnapshot.max())).append("\n");
        ContinuousMetricSnapshot interleavesForDependencyOperationsSnapshot = interleavesForDependencyOperations.snapshot();
        String minInterleaveForDependencyOperations = (interleavesForDependencyOperationsSnapshot.count() == 0)
                ?
                "--"
                :
                temporalUtil.milliDurationToString(Math.round(interleavesForDependencyOperationsSnapshot.min()));
        String meanInterleaveForDependencyOperations = (interleavesForDependencyOperationsSnapshot.count() == 0)
                ?
                "--"
                :
                temporalUtil.milliDurationToString(Math.round(interleavesForDependencyOperationsSnapshot.mean()));
        String maxInterleaveForDependencyOperations = (interleavesForDependencyOperationsSnapshot.count() == 0)
                ?
                "--"
                :
                temporalUtil.milliDurationToString(Math.round(interleavesForDependencyOperationsSnapshot.max()));
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependency Operations:")).
                append("min = ").append(minInterleaveForDependencyOperations).append(" / ").
                append("mean = ").append(meanInterleaveForDependencyOperations).append(" / ").
                append("max = ").append(maxInterleaveForDependencyOperations).append("\n");
        ContinuousMetricSnapshot interleavesForDependentOperationsSnapshot = interleavesForDependentOperations.snapshot();
        String minInterleaveForDependentOperations = (interleavesForDependentOperationsSnapshot.count() == 0)
                ?
                "--"
                :
                temporalUtil.milliDurationToString(Math.round(interleavesForDependentOperationsSnapshot.min()));
        String meanInterleaveForDependentOperations = (interleavesForDependentOperationsSnapshot.count() == 0)
                ?
                "--"
                :
                temporalUtil.milliDurationToString(Math.round(interleavesForDependentOperationsSnapshot.mean()));
        String maxInterleaveForDependentOperations = (interleavesForDependentOperationsSnapshot.count() == 0)
                ?
                "--"
                :
                temporalUtil.milliDurationToString(Math.round(interleavesForDependentOperationsSnapshot.max()));
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependent Operations:")).
                append("min = ").append(minInterleaveForDependentOperations).append(" / ").
                append("mean = ").append(meanInterleaveForDependentOperations).append(" / ").
                append("max = ").append(maxInterleaveForDependentOperations).append("\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("  BY OPERATION TYPE\n");
        sb.append("  ------------------------------------------------------\n");
        for (Map.Entry<Class, Long> lowestDependencyDurationAsMilliForOperationType : MapUtils.sortedEntrySet(lowestDependencyDurationAsMilliByOperationType())) {
            Class<Operation<?>> operationType = lowestDependencyDurationAsMilliForOperationType.getKey();
            long firstStartAsMilliTypeForOperationType = firstStartTimesAsMilliByOperationType().get(operationType);
            long lastStartAsMilliTypeForOperationType = lastStartTimesAsMilliByOperationType().get(operationType);
            sb.append(String.format("%1$-" + padRightDistance + "s", "     " + operationType.getSimpleName() + ":")).
                    append("Min Dependency Duration(").append(lowestDependencyDurationAsMilliForOperationType.getValue()).append(") ");
            if (operationInterleavesByOperationType().containsKey(operationType)) {
                ContinuousMetricSnapshot interleavesForOperationTypeSnapshot = operationInterleavesByOperationType().get(operationType).snapshot();
                sb.
                        append("Time Span(").
                        append(firstStartAsMilliTypeForOperationType).append(", ").append(lastStartAsMilliTypeForOperationType).append(") ").
                        append("Interleave(").
                        append("min = ").append(temporalUtil.milliDurationToString(interleavesForOperationTypeSnapshot.min())).append(" / ").
                        append("mean = ").append(temporalUtil.milliDurationToString(Math.round(interleavesForOperationTypeSnapshot.mean()))).append(" / ").
                        append("max = ").append(temporalUtil.milliDurationToString(interleavesForOperationTypeSnapshot.max())).append(")");
            }
            sb.append("\n");
        }
        sb.append("********************************************************");
        return sb.toString();
    }

    private List<String> toSortedClassNames(Iterable<Class> classes) {
        List<String> classNames = Lists.newArrayList(
                Iterables.transform(
                        classes,
                        new Function<Class, String>() {
                            @Override
                            public String apply(Class aClass) {
                                return aClass.getSimpleName();
                            }
                        })
        );
        Collections.sort(classNames);
        return classNames;
    }
}