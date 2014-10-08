package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.MapUtils;

import java.util.Map;
import java.util.Set;

public class WorkloadStatistics {
    private final Map<Class, Time> firstStartTimesByOperationType;
    private final Map<Class, Time> lastStartTimesByOperationType;
    private final Histogram<Class, Long> operationMixHistogram;
    private final ContinuousMetricManager operationInterleaves;
    private final ContinuousMetricManager interleavesForDependencyOperations;
    private final ContinuousMetricManager interleavesForDependentOperations;
    private final Map<Class, ContinuousMetricManager> operationInterleavesByOperationType;
    private final Set<Class> dependencyOperationTypes;
    private final Set<Class> dependentOperationTypes;
    private final Map<Class, Duration> lowestDependencyDurationByOperationType;

    public WorkloadStatistics(Map<Class, Time> firstStartTimesByOperationType,
                              Map<Class, Time> lastStartTimesByOperationType,
                              Histogram<Class, Long> operationMixHistogram,
                              ContinuousMetricManager operationInterleaves,
                              ContinuousMetricManager interleavesForDependencyOperations,
                              ContinuousMetricManager interleavesForDependentOperations,
                              Map<Class, ContinuousMetricManager> operationInterleavesByOperationType,
                              Set<Class> dependencyOperationTypes,
                              Set<Class> dependentOperationTypes,
                              Map<Class, Duration> lowestDependencyDurationByOperationType) {
        this.firstStartTimesByOperationType = firstStartTimesByOperationType;
        this.lastStartTimesByOperationType = lastStartTimesByOperationType;
        this.operationMixHistogram = operationMixHistogram;
        this.operationInterleaves = operationInterleaves;
        this.interleavesForDependencyOperations = interleavesForDependencyOperations;
        this.interleavesForDependentOperations = interleavesForDependentOperations;
        this.operationInterleavesByOperationType = operationInterleavesByOperationType;
        this.dependencyOperationTypes = dependencyOperationTypes;
        this.dependentOperationTypes = dependentOperationTypes;
        this.lowestDependencyDurationByOperationType = lowestDependencyDurationByOperationType;
    }

    public long totalCount() {
        long count = 0;
        for (Map.Entry<Class, ContinuousMetricManager> operationInterleaveForOperationType : operationInterleavesByOperationType.entrySet()) {
            count += operationInterleaveForOperationType.getValue().snapshot().count();
            // because interleaves are the durations BETWEEN operation occurrences they will be off by one
            // if there are no occurrences there will be no start time for the operation type
            // if there ARE occurrences, we should increment count by 1
            if (firstStartTimesByOperationType.containsKey(operationInterleaveForOperationType.getKey()))
                count += 1;
        }
        return count;
    }

    public Duration totalDuration() {
        Time firstStartTime = firstStartTime();
        if (null == firstStartTime) return null;
        Time lastStartTime = lastStartTime();
        if (null == lastStartTime) return null;
        return lastStartTime.durationGreaterThan(firstStartTime);
    }

    public int operationTypeCount() {
        return Math.max(firstStartTimesByOperationType().size(), operationMix().getBucketCount());
    }

    public Time firstStartTime() {
        Time firstStartTime = null;
        for (Map.Entry<Class, Time> firstStartTimeForOperationType : firstStartTimesByOperationType.entrySet()) {
            if (null == firstStartTime || firstStartTimeForOperationType.getValue().lt(firstStartTime))
                firstStartTime = firstStartTimeForOperationType.getValue();
        }
        return firstStartTime;
    }

    public Time lastStartTime() {
        Time lastStartTime = null;
        for (Map.Entry<Class, Time> lastStartTimeForOperationType : lastStartTimesByOperationType.entrySet()) {
            if (null == lastStartTime || lastStartTimeForOperationType.getValue().gt(lastStartTime))
                lastStartTime = lastStartTimeForOperationType.getValue();
        }
        return lastStartTime;
    }

    public Map<Class, Time> firstStartTimesByOperationType() {
        return firstStartTimesByOperationType;
    }

    public Map<Class, Time> lastStartTimesByOperationType() {
        return lastStartTimesByOperationType;
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

    public Map<Class, Duration> lowestDependencyDurationByOperationType() {
        return lowestDependencyDurationByOperationType;
    }

    @Override
    public String toString() {
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
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Total Duration:")).append(totalDuration()).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Time Span:")).append(firstStartTime()).append(", ").append(lastStartTime()).append("\n");
        ContinuousMetricSnapshot interleavesSnapshot = operationInterleaves().snapshot();
        sb.append(String.format("%1$-" + padRightDistance + "s", "     Interleaves:")).
                append("min = ").append(Duration.fromMilli(interleavesSnapshot.min())).append(" / ").
                append("mean = ").append(Duration.fromMilli(Math.round(interleavesSnapshot.mean()))).append(" / ").
                append("max = ").append(Duration.fromMilli(interleavesSnapshot.max())).append("\n");
        sb.append("     Operation Mix:\n");
        for (Map.Entry<Bucket<Class>, Long> operationMixForOperationType : MapUtils.sortedEntries(operationMix().getAllBuckets())) {
            Bucket.DiscreteBucket<Class> bucket = (Bucket.DiscreteBucket<Class>) operationMixForOperationType.getKey();
            Class<Operation<?>> operationType = bucket.getId();
            long operationCount = operationMixForOperationType.getValue();
            sb.append(String.format("%1$-" + padRightDistance + "s", "        " + operationType.getSimpleName() + ":")).append(operationCount).append("\n");
        }
        sb.append("     Operation Dependency Modes:\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependency Operations:")).append(dependencyOperationTypes.toString()).append("\n");
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependent Operations:")).append(dependentOperationTypes.toString()).append("\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("  BY DEPENDENCY MODE\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("     Interleaves:\n");
        ContinuousMetricSnapshot interleavesForDependencyOperationsSnapshot = interleavesForDependencyOperations.snapshot();
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependency Operations:")).
                append("min = ").append(Duration.fromMilli(interleavesForDependencyOperationsSnapshot.min())).append(" / ").
                append("mean =").append(Duration.fromMilli(Math.round(interleavesForDependencyOperationsSnapshot.mean()))).append(" / ").
                append("max =").append(Duration.fromMilli(interleavesForDependencyOperationsSnapshot.max())).append("\n");
        ContinuousMetricSnapshot interleavesForDependentOperationsSnapshot = interleavesForDependentOperations.snapshot();
        sb.append(String.format("%1$-" + padRightDistance + "s", "        Dependent Operations:")).
                append("min = ").append(Duration.fromMilli(interleavesForDependentOperationsSnapshot.min())).append(" / ").
                append("mean =").append(Duration.fromMilli(Math.round(interleavesForDependentOperationsSnapshot.mean()))).append(" / ").
                append("max =").append(Duration.fromMilli(interleavesForDependentOperationsSnapshot.max())).append("\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("  BY OPERATION TYPE\n");
        sb.append("  ------------------------------------------------------\n");
        for (Map.Entry<Class, Duration> lowestDependencyDurationForOperationType : MapUtils.sortedEntrySet(lowestDependencyDurationByOperationType())) {
            Class<Operation<?>> operationType = lowestDependencyDurationForOperationType.getKey();
            Time firstStartTypeForOperationType = firstStartTimesByOperationType().get(operationType);
            Time lastStartTypeForOperationType = lastStartTimesByOperationType().get(operationType);
            sb.append(String.format("%1$-" + padRightDistance + "s", "     " + operationType.getSimpleName() + ":")).
                    append("Min Dependency Duration(").append(lowestDependencyDurationForOperationType.getValue()).append(") ");
            if (operationInterleavesByOperationType().containsKey(operationType)) {
                ContinuousMetricSnapshot interleavesForOperationTypeSnapshot = operationInterleavesByOperationType().get(operationType).snapshot();
                sb.
                        append("Time Span(").
                        append(firstStartTypeForOperationType).append(", ").append(lastStartTypeForOperationType).append(") ").
                        append("Interleave(").
                        append("min = ").append(Duration.fromMilli(interleavesForOperationTypeSnapshot.min())).append(" / ").
                        append("mean = ").append(Duration.fromMilli(Math.round(interleavesForOperationTypeSnapshot.mean()))).append(" / ").
                        append("max = ").append(Duration.fromMilli(interleavesForOperationTypeSnapshot.max())).append(")");
            }
            sb.append("\n");
        }
        sb.append("********************************************************");
        return sb.toString();
    }
}