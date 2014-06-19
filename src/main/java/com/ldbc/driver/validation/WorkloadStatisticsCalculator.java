package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.OperationClassification.GctMode;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;

import java.util.*;

public class WorkloadStatisticsCalculator {
    /**
     * TODO
     * - driver parameter that allows workload summary to be checked and printed out, along with gct, and warning if GCT appears to be too short;
     * - optionally add gct status to status printout
     * TODO test HdrHistogram limits
     * TODO possibly DbValidator class too:
     * - is there a handler for every operation type
     * TODO generator that creates an operation stream based on total time, rather than count
     */

    public WorkloadStatistics calculate(Iterator<Operation<?>> operations,
                                        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                                        Duration maxExpectedInterleave) throws MetricsCollectionException {
        Histogram<Class, Long> operationMixHistogram = new Histogram<>(0l);

        ContinuousMetricManager operationInterleaves = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);

        Time previousOperationStartTime = null;

        Map<OperationClassification.GctMode, Time> previousOperationStartTimesByGctMode = new HashMap<>();
        Map<OperationClassification.GctMode, ContinuousMetricManager> operationInterleavesByGctMode = new HashMap<>();

        Map<Class, Time> previousOperationStartTimesByOperationType = new HashMap<>();
        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        Map<Class, Time> firstStartTimesByOperationType = new HashMap<>();
        Map<Class, Time> lastStartTimesByOperationType = new HashMap<>();

        Map<GctMode, Set<Class>> operationsByGctMode = new HashMap<>();
        for (Map.Entry<Class<? extends Operation<?>>, OperationClassification> operationClassificationEntry : operationClassifications.entrySet()) {
            Class operationType = operationClassificationEntry.getKey();
            GctMode operationGctMode = operationClassificationEntry.getValue().gctMode();
            Set<Class> operationsForGctMode;
            if (operationsByGctMode.containsKey(operationGctMode))
                operationsForGctMode = operationsByGctMode.get(operationGctMode);
            else {
                operationsForGctMode = new HashSet<>();
                operationsByGctMode.put(operationGctMode, operationsForGctMode);
            }
            operationsForGctMode.add(operationType);
        }

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            Class operationType = operation.getClass();
            Time operationStartTime = operation.scheduledStartTime();
            OperationClassification operationClassification = operationClassifications.get(operationType);
            OperationClassification.GctMode operationGctMode = operationClassification.gctMode();

            // Operation Mix
            operationMixHistogram.incOrCreateBucket(Bucket.DiscreteBucket.create(operationType), 1l);

            // Interleaves
            if (null != previousOperationStartTime) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTime);
                operationInterleaves.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTime = operationStartTime;

            // Interleaves by GCT mode
            ContinuousMetricManager operationInterleaveForGctMode = operationInterleavesByGctMode.get(operationGctMode);
            if (null == operationInterleaveForGctMode) {
                operationInterleaveForGctMode = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
                operationInterleavesByGctMode.put(operationGctMode, operationInterleaveForGctMode);
            }
            Time previousOperationStartTimeByGctMode = previousOperationStartTimesByGctMode.get(operationGctMode);
            if (null != previousOperationStartTimeByGctMode) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTimeByGctMode);
                operationInterleaveForGctMode.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTimesByGctMode.put(operationGctMode, operationStartTime);

            // Interleaves by operation type
            ContinuousMetricManager operationInterleaveForOperationType = operationInterleavesByOperationType.get(operationType);
            if (null == operationInterleaveForOperationType) {
                operationInterleaveForOperationType = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
                operationInterleavesByOperationType.put(operationType, operationInterleaveForOperationType);
            }
            Time previousOperationStartTimeByOperationType = previousOperationStartTimesByOperationType.get(operationType);
            if (null != previousOperationStartTimeByOperationType) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTimeByOperationType);
                operationInterleaveForOperationType.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTimesByOperationType.put(operationType, operationStartTime);

            // First start times by operation type
            if (false == firstStartTimesByOperationType.containsKey(operationType))
                firstStartTimesByOperationType.put(operationType, operationStartTime);

            // Last start times by operation type
            lastStartTimesByOperationType.put(operationType, operationStartTime);
        }

        return new WorkloadStatistics(
                firstStartTimesByOperationType,
                lastStartTimesByOperationType,
                operationMixHistogram,
                operationInterleaves,
                operationInterleavesByGctMode,
                operationInterleavesByOperationType,
                operationsByGctMode);
    }
}
