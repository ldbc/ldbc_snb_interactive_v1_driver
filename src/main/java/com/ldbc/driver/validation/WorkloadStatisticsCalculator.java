package com.ldbc.driver.validation;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;

import java.util.*;

public class WorkloadStatisticsCalculator {
    /**
     * TODO generator that creates an operation stream based on total time, rather than count
     * TODO report how frequently GCT is updated
     */

    public WorkloadStatistics calculate(WorkloadStreams workloadStreams,
                                        Duration maxExpectedInterleave) throws MetricsCollectionException {
        Histogram<Class, Long> operationMixHistogram = new Histogram<>(0l);
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        ContinuousMetricManager operationInterleaves = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);

        Time previousOperationStartTime = null;

        final Map<Class, Time> previousOperationStartTimesByOperationType = new HashMap<>();
        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        Map<Class, Time> firstStartTimesByOperationType = new HashMap<>();
        Map<Class, Time> lastStartTimesByOperationType = new HashMap<>();

        final Set<Class> dependencyOperationTypes = new HashSet<>();
        final Set<Class> dependentOperationTypes = new HashSet<>();
        final ContinuousMetricManager interleavesForDependencyOperations = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
        final ContinuousMetricManager interleavesForDependentOperations = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);

        dependentOperationTypes.addAll(workloadStreams.asynchronousStream().dependentOperationTypes());
        for (WorkloadStreams.WorkloadStreamDefinition streamDefinition : workloadStreams.blockingStreamDefinitions()) {
            dependentOperationTypes.addAll(streamDefinition.dependentOperationTypes());
        }

        List<Iterator<Operation<?>>> dependencyOperationIterators = new ArrayList<>();
        dependencyOperationIterators.add(workloadStreams.asynchronousStream().dependencyOperations());
        for (WorkloadStreams.WorkloadStreamDefinition streamDefinition : workloadStreams.blockingStreamDefinitions()) {
            dependencyOperationIterators.add(streamDefinition.dependencyOperations());
        }
        List<Iterator<Operation<?>>> nonDependencyOperationIterators = new ArrayList<>();
        nonDependencyOperationIterators.add(workloadStreams.asynchronousStream().nonDependencyOperations());
        for (WorkloadStreams.WorkloadStreamDefinition streamDefinition : workloadStreams.blockingStreamDefinitions()) {
            nonDependencyOperationIterators.add(streamDefinition.nonDependencyOperations());
        }

        Function<Operation<?>, Operation<?>> collectStatsForDependencyOperations = new Function<Operation<?>, Operation<?>>() {
            Time prevDependency = null;
            Time prevDependent = null;

            @Override
            public Operation<?> apply(Operation<?> operation) {
                dependencyOperationTypes.add(operation.getClass());
                if (null == prevDependency) {
                    prevDependency = operation.scheduledStartTime();
                } else {
                    long interleaveAsMilli = operation.scheduledStartTime().durationGreaterThan(prevDependency).asMilli();
                    try {
                        interleavesForDependencyOperations.addMeasurement(interleaveAsMilli);
                    } catch (MetricsCollectionException e) {
                        throw new RuntimeException("Error collectStatsForDependencyOperations", e);
                    }
                    prevDependency = operation.scheduledStartTime();
                }
                if (dependentOperationTypes.contains(operation.getClass())) {
                    if (null == prevDependent) {
                        prevDependent = operation.scheduledStartTime();
                    } else {
                        long interleaveAsMilli = operation.scheduledStartTime().durationGreaterThan(prevDependent).asMilli();
                        try {
                            interleavesForDependentOperations.addMeasurement(interleaveAsMilli);
                        } catch (MetricsCollectionException e) {
                            throw new RuntimeException("Error collectStatsForNonDependentOperations", e);
                        }
                        prevDependent = operation.scheduledStartTime();
                    }
                }
                return operation;
            }
        };
        Iterator<Operation<?>> dependencyOperations = Iterators.transform(
                gf.mergeSortOperationsByStartTime(dependencyOperationIterators.toArray(new Iterator[dependencyOperationIterators.size()])),
                collectStatsForDependencyOperations

        );

        Function<Operation<?>, Operation<?>> collectStatsForNonDependentOperations = new Function<Operation<?>, Operation<?>>() {
            Time prevDependent = null;

            @Override
            public Operation<?> apply(Operation<?> operation) {
                if (dependentOperationTypes.contains(operation.getClass())) {
                    if (null == prevDependent) {
                        prevDependent = operation.scheduledStartTime();
                    } else {
                        long interleaveAsMilli = operation.scheduledStartTime().durationGreaterThan(prevDependent).asMilli();
                        try {
                            interleavesForDependentOperations.addMeasurement(interleaveAsMilli);
                        } catch (MetricsCollectionException e) {
                            throw new RuntimeException("Error collectStatsForNonDependentOperations", e);
                        }
                        prevDependent = operation.scheduledStartTime();
                    }
                }
                return operation;
            }
        };
        Iterator<Operation<?>> nonDependencyOperations = Iterators.transform(
                gf.mergeSortOperationsByStartTime(nonDependencyOperationIterators.toArray(new Iterator[nonDependencyOperationIterators.size()])),
                collectStatsForNonDependentOperations
        );

        Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(dependencyOperations, nonDependencyOperations);

        Map<Class, Duration> lowestDependencyDurationByOperationType = new HashMap<>();

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            Class operationType = operation.getClass();
            Time operationStartTime = operation.scheduledStartTime();
            Time operationDependencyTime = operation.dependencyTime();
            Duration operationDependencyDuration = operationStartTime.durationGreaterThan(operationDependencyTime);

            // Operation Mix
            operationMixHistogram.incOrCreateBucket(Bucket.DiscreteBucket.create(operationType), 1l);

            // Interleaves
            if (null != previousOperationStartTime) {
                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTime);
                operationInterleaves.addMeasurement(interleaveDuration.asMilli());
            }
            previousOperationStartTime = operationStartTime;

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
                interleavesForDependencyOperations,
                interleavesForDependentOperations,
                operationInterleavesByOperationType,
                dependencyOperationTypes,
                dependentOperationTypes,
                lowestDependencyDurationByOperationType);
    }
}
