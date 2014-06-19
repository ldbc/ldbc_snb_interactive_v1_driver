package com.ldbc.driver.validation;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.OperationClassification.GctMode;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.ContinuousMetricSnapshot;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WorkloadStatistics {
    private final Histogram<Class, Long> operationMixHistogram;
    private final ContinuousMetricManager operationInterleaves;
    private final Map<OperationClassification.GctMode, ContinuousMetricManager> operationInterleavesByGctMode;
    private final Map<Class, ContinuousMetricManager> operationInterleavesByOperationType;
    private final Map<Class, Time> firstStartTimesByOperationType;
    private final Map<Class, Time> lastStartTimesByOperationType;
    private final Map<GctMode, Set<Class>> operationsByGctMode;

    public WorkloadStatistics(Map<Class, Time> firstStartTimesByOperationType,
                              Map<Class, Time> lastStartTimesByOperationType,
                              Histogram<Class, Long> operationMixHistogram,
                              ContinuousMetricManager operationInterleaves,
                              Map<OperationClassification.GctMode, ContinuousMetricManager> operationInterleavesByGctMode,
                              Map<Class, ContinuousMetricManager> operationInterleavesByOperationType,
                              Map<GctMode, Set<Class>> operationsByGctMode) {
        this.firstStartTimesByOperationType = firstStartTimesByOperationType;
        this.lastStartTimesByOperationType = lastStartTimesByOperationType;
        this.operationMixHistogram = operationMixHistogram;
        this.operationInterleaves = operationInterleaves;
        this.operationInterleavesByGctMode = operationInterleavesByGctMode;
        this.operationInterleavesByOperationType = operationInterleavesByOperationType;
        this.operationsByGctMode = operationsByGctMode;

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
        return lastStartTime.greaterBy(firstStartTime);
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

    public Map<OperationClassification.GctMode, ContinuousMetricManager> operationInterleavesByGctMode() {
        return operationInterleavesByGctMode;
    }

    public Map<Class, ContinuousMetricManager> operationInterleavesByOperationType() {
        return operationInterleavesByOperationType;
    }

    public Map<GctMode, Set<Class>> operationsByGctMode() {
        return operationsByGctMode;
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
        for (Map.Entry<Bucket<Class>, Long> operationMixForOperationType : operationMix().getAllBuckets()) {
            Bucket.DiscreteBucket<Class> bucket = (Bucket.DiscreteBucket<Class>) operationMixForOperationType.getKey();
            Class<Operation<?>> operationType = bucket.getId();
            long operationCount = operationMixForOperationType.getValue();
            sb.append(String.format("%1$-" + padRightDistance + "s", "        " + operationType.getSimpleName() + ":")).append(operationCount).append("\n");
        }
        sb.append("     Operation GCT Modes:\n");
        for (Map.Entry<GctMode, Set<Class>> operationsInGctMode : operationsByGctMode().entrySet()) {
            GctMode gctMode = operationsInGctMode.getKey();
            List<String> operationNames = Lists.newArrayList(Iterables.transform(operationsInGctMode.getValue(), new Function<Class, String>() {
                @Override
                public String apply(Class operationType) {
                    return operationType.getSimpleName();
                }
            }));
            Collections.sort(operationNames);
            sb.append(String.format("%1$-" + padRightDistance + "s", "        " + gctMode + ":")).append(operationNames.toString()).append("\n");
        }
        sb.append("  ------------------------------------------------------\n");
        sb.append("  BY GCT MODE\n");
        sb.append("  ------------------------------------------------------\n");
        sb.append("     Interleaves:\n");
        for (Map.Entry<GctMode, ContinuousMetricManager> interleavesForGctMode : operationInterleavesByGctMode().entrySet()) {
            GctMode gctMode = interleavesForGctMode.getKey();
            ContinuousMetricSnapshot interleavesForGctModeSnapshot = interleavesForGctMode.getValue().snapshot();
            sb.append(String.format("%1$-" + padRightDistance + "s", "        " + gctMode + ":")).
                    append("min = ").append(Duration.fromMilli(interleavesForGctModeSnapshot.min())).append(" / ").
                    append("mean =").append(Duration.fromMilli(Math.round(interleavesForGctModeSnapshot.mean()))).append(" / ").
                    append("max =").append(Duration.fromMilli(interleavesForGctModeSnapshot.max())).append("\n");
        }
        sb.append("  ------------------------------------------------------\n");
        sb.append("  BY OPERATION TYPE\n");
        sb.append("  ------------------------------------------------------\n");
        for (Map.Entry<Class, ContinuousMetricManager> interleavesForOperationType : operationInterleavesByOperationType().entrySet()) {
            Class<Operation<?>> operationType = interleavesForOperationType.getKey();
            Time firstStartTypeForOperationType = firstStartTimesByOperationType().get(operationType);
            Time lastStartTypeForOperationType = lastStartTimesByOperationType().get(operationType);
            ContinuousMetricSnapshot interleavesForOperationTypeSnapshot = interleavesForOperationType.getValue().snapshot();
            sb.append(String.format("%1$-" + padRightDistance + "s", "     " + operationType.getSimpleName() + ":")).
                    append("Time Span(").
                    append(firstStartTypeForOperationType).append(", ").append(lastStartTypeForOperationType).append(") ").
                    append("Interleave(").
                    append("min = ").append(Duration.fromMilli(interleavesForOperationTypeSnapshot.min())).append(" / ").
                    append("mean = ").append(Duration.fromMilli(Math.round(interleavesForOperationTypeSnapshot.mean()))).append(" / ").
                    append("max = ").append(Duration.fromMilli(interleavesForOperationTypeSnapshot.max())).append(")\n");
        }
        sb.append("********************************************************");
        return sb.toString();
    }
}