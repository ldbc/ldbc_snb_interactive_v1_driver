package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;

import java.util.*;

public class WorkloadStatisticsCalculator {
    /**
     * TODO test HdrHistogram limits
     * <p/>
     * TODO generator that creates an operation stream based on total time, rather than count
     * <p/>
     * TODO Group By SchedulingMode (like is already done by GctMode)
     * <p/>
     * TODO report how frequently GCT is updated
     * <p/>
     * TODO report how frequently GCT is read
     * <p/>
     * TODO report how many operations are in each window (min, max, mean, percentiles)
     * TODO -- to support this a DriverConfiguration needs to be passed in, and WindowSize needs to be added to DriverConfiguration
     */

    public WorkloadStatistics calculate(Iterator<Operation<?>> operations,
                                        Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                        Duration maxExpectedInterleave) throws MetricsCollectionException {
        Histogram<Class, Long> operationMixHistogram = new Histogram<>(0l);

        ContinuousMetricManager operationInterleaves = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);

        Time previousOperationStartTime = null;

        Map<OperationClassification.DependencyMode, Time> previousOperationStartTimesByDependencyMode = new HashMap<>();
        Map<OperationClassification.DependencyMode, ContinuousMetricManager> operationInterleavesByDependencyMode = new HashMap<>();

        Map<Class, Time> previousOperationStartTimesByOperationType = new HashMap<>();
        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        Map<Class, Time> firstStartTimesByOperationType = new HashMap<>();
        Map<Class, Time> lastStartTimesByOperationType = new HashMap<>();

        Map<OperationClassification.DependencyMode, Set<Class>> operationsByDependencyMode = new HashMap<>();
        for (Map.Entry<Class<? extends Operation>, OperationClassification> operationClassificationEntry : operationClassifications.entrySet()) {
            Class operationType = operationClassificationEntry.getKey();
            OperationClassification.DependencyMode operationDependencyMode = operationClassificationEntry.getValue().dependencyMode();
            Set<Class> operationsForDependencyMode;
            if (operationsByDependencyMode.containsKey(operationDependencyMode))
                operationsForDependencyMode = operationsByDependencyMode.get(operationDependencyMode);
            else {
                operationsForDependencyMode = new HashSet<>();
                operationsByDependencyMode.put(operationDependencyMode, operationsForDependencyMode);
            }
            operationsForDependencyMode.add(operationType);
        }

        Map<Class, Duration> lowestDependencyDurationByOperationType = new HashMap<>();

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            Class operationType = operation.getClass();
            Time operationStartTime = operation.scheduledStartTime();
            Time operationDependencyTime = operation.dependencyTime();
            Duration operationDependencyDuration = operationStartTime.durationGreaterThan(operationDependencyTime);
            OperationClassification operationClassification = operationClassifications.get(operationType);
            OperationClassification.DependencyMode operationDependencyMode = operationClassification.dependencyMode();
            // TODO use
            OperationClassification.SchedulingMode operationSchedulingMode = operationClassification.schedulingMode();

            // Operation Mix
            operationMixHistogram.incOrCreateBucket(Bucket.DiscreteBucket.create(operationType), 1l);

            // Interleaves
            if (null != previousOperationStartTime) {
                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTime);
                operationInterleaves.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTime = operationStartTime;

            // Interleaves by dependency mode
            ContinuousMetricManager operationInterleaveForDependencyMode = operationInterleavesByDependencyMode.get(operationDependencyMode);
            if (null == operationInterleaveForDependencyMode) {
                operationInterleaveForDependencyMode = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
                operationInterleavesByDependencyMode.put(operationDependencyMode, operationInterleaveForDependencyMode);
            }
            Time previousOperationStartTimeByDependencyMode = previousOperationStartTimesByDependencyMode.get(operationDependencyMode);
            if (null != previousOperationStartTimeByDependencyMode) {
                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTimeByDependencyMode);
                operationInterleaveForDependencyMode.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTimesByDependencyMode.put(operationDependencyMode, operationStartTime);

            // Interleaves by operation type
            ContinuousMetricManager operationInterleaveForOperationType = operationInterleavesByOperationType.get(operationType);
            if (null == operationInterleaveForOperationType) {
                operationInterleaveForOperationType = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
                operationInterleavesByOperationType.put(operationType, operationInterleaveForOperationType);
            }
            Time previousOperationStartTimeForOperationType = previousOperationStartTimesByOperationType.get(operationType);
            if (null != previousOperationStartTimeForOperationType) {
                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTimeForOperationType);
                operationInterleaveForOperationType.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTimesByOperationType.put(operationType, operationStartTime);

            // Dependency duration by operation type
            Duration lowestDependencyDurationForOperationType = (lowestDependencyDurationByOperationType.containsKey(operationType))
                    ? lowestDependencyDurationByOperationType.get(operationType)
                    : Duration.fromNano(Long.MAX_VALUE);
            if (operationDependencyDuration.lt(lowestDependencyDurationForOperationType))
                lowestDependencyDurationByOperationType.put(operationType, operationDependencyDuration);

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
                operationInterleavesByDependencyMode,
                operationInterleavesByOperationType,
                operationsByDependencyMode,
                lowestDependencyDurationByOperationType);
    }
}
