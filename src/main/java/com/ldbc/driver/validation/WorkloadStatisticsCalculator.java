package com.ldbc.driver.validation;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;

import java.util.*;

public class WorkloadStatisticsCalculator {
    /**
     * TODO generator that creates an operation stream based on total time, rather than count
     * TODO report how frequently GCT is updated
     */

    public WorkloadStatistics calculate(WorkloadStreams workloadStreams,
                                        long maxExpectedInterleaveAsMilli) throws MetricsCollectionException {
        Histogram<Class, Long> operationMixHistogram = new Histogram<>(0l);
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        ContinuousMetricManager operationInterleaves = new ContinuousMetricManager(null, null, maxExpectedInterleaveAsMilli, 5);

        long previousOperationStartTimeAsMilli = -1;

        final Map<Class, Long> previousOperationStartTimesAsMilliByOperationType = new HashMap<>();
        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        Map<Class, Long> firstStartTimesAsMilliByOperationType = new HashMap<>();
        Map<Class, Long> lastStartTimesAsMilliByOperationType = new HashMap<>();

        final Set<Class> dependencyOperationTypes = new HashSet<>();
        final Set<Class> dependentOperationTypes = new HashSet<>();
        final ContinuousMetricManager interleavesForDependencyOperations = new ContinuousMetricManager(null, null, maxExpectedInterleaveAsMilli, 5);
        final ContinuousMetricManager interleavesForDependentOperations = new ContinuousMetricManager(null, null, maxExpectedInterleaveAsMilli, 5);

        if (workloadStreams.asynchronousStream().dependencyOperations().hasNext() || workloadStreams.asynchronousStream().nonDependencyOperations().hasNext())
            dependentOperationTypes.addAll(workloadStreams.asynchronousStream().dependentOperationTypes());
        for (WorkloadStreams.WorkloadStreamDefinition streamDefinition : workloadStreams.blockingStreamDefinitions()) {
            if (streamDefinition.dependencyOperations().hasNext() || streamDefinition.nonDependencyOperations().hasNext())
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
            long prevDependencyAsMilli = -1;
            long prevDependentAsMilli = -1;

            @Override
            public Operation<?> apply(Operation<?> operation) {
                dependencyOperationTypes.add(operation.getClass());
                if (-1 == prevDependencyAsMilli) {
                    prevDependencyAsMilli = operation.scheduledStartTimeAsMilli();
                } else {
                    long interleaveAsMilli = operation.scheduledStartTimeAsMilli() - prevDependencyAsMilli;
                    try {
                        interleavesForDependencyOperations.addMeasurement(interleaveAsMilli);
                    } catch (MetricsCollectionException e) {
                        throw new RuntimeException("Error collectStatsForDependencyOperations", e);
                    }
                    prevDependencyAsMilli = operation.scheduledStartTimeAsMilli();
                }
                if (dependentOperationTypes.contains(operation.getClass())) {
                    if (-1 == prevDependentAsMilli) {
                        prevDependentAsMilli = operation.scheduledStartTimeAsMilli();
                    } else {
                        long interleaveAsMilli = operation.scheduledStartTimeAsMilli() - prevDependentAsMilli;
                        try {
                            interleavesForDependentOperations.addMeasurement(interleaveAsMilli);
                        } catch (MetricsCollectionException e) {
                            throw new RuntimeException("Error collectStatsForNonDependentOperations", e);
                        }
                        prevDependentAsMilli = operation.scheduledStartTimeAsMilli();
                    }
                }
                return operation;
            }
        };
        Iterator<Operation<?>> dependencyOperations = Iterators.transform(
                gf.mergeSortOperationsByTimeStamp(dependencyOperationIterators.toArray(new Iterator[dependencyOperationIterators.size()])),
                collectStatsForDependencyOperations
        );

        Function<Operation<?>, Operation<?>> collectStatsForNonDependentOperations = new Function<Operation<?>, Operation<?>>() {
            long prevDependentAsMilli = -1;

            @Override
            public Operation<?> apply(Operation<?> operation) {
                if (dependentOperationTypes.contains(operation.getClass())) {
                    if (-1 == prevDependentAsMilli) {
                        prevDependentAsMilli = operation.scheduledStartTimeAsMilli();
                    } else {
                        long interleaveAsMilli = operation.scheduledStartTimeAsMilli() - prevDependentAsMilli;
                        try {
                            interleavesForDependentOperations.addMeasurement(interleaveAsMilli);
                        } catch (MetricsCollectionException e) {
                            throw new RuntimeException("Error collectStatsForNonDependentOperations", e);
                        }
                        prevDependentAsMilli = operation.scheduledStartTimeAsMilli();
                    }
                }
                return operation;
            }
        };
        Iterator<Operation<?>> nonDependencyOperations = Iterators.transform(
                gf.mergeSortOperationsByTimeStamp(nonDependencyOperationIterators.toArray(new Iterator[nonDependencyOperationIterators.size()])),
                collectStatsForNonDependentOperations
        );

        Iterator<Operation<?>> operations = gf.mergeSortOperationsByTimeStamp(dependencyOperations, nonDependencyOperations);

        Map<Class, Long> lowestDependencyDurationAsMilliByOperationType = new HashMap<>();

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            Class operationType = operation.getClass();
            long operationStartTimeAsMilli = operation.scheduledStartTimeAsMilli();
            long operationDependencyTimeAsMilli = operation.dependencyTimeStamp();
            long operationDependencyDurationAsMilli = operationStartTimeAsMilli - operationDependencyTimeAsMilli;

            // Operation Mix
            operationMixHistogram.incOrCreateBucket(Bucket.DiscreteBucket.create(operationType), 1l);

            // Interleaves
            if (-1 != previousOperationStartTimeAsMilli) {
                long interleaveDurationAsMilli = operationStartTimeAsMilli - previousOperationStartTimeAsMilli;
                operationInterleaves.addMeasurement(interleaveDurationAsMilli);
            }
            previousOperationStartTimeAsMilli = operationStartTimeAsMilli;

            // Interleaves by operation type
            ContinuousMetricManager operationInterleaveForOperationType = operationInterleavesByOperationType.get(operationType);
            if (null == operationInterleaveForOperationType) {
                operationInterleaveForOperationType = new ContinuousMetricManager(null, null, maxExpectedInterleaveAsMilli, 5);
                operationInterleavesByOperationType.put(operationType, operationInterleaveForOperationType);
            }
            Long previousOperationStartTimeAsMilliForOperationType = previousOperationStartTimesAsMilliByOperationType.get(operationType);
            if (null != previousOperationStartTimeAsMilliForOperationType) {
                long interleaveDurationAsMilli = operationStartTimeAsMilli - previousOperationStartTimeAsMilliForOperationType;
                operationInterleaveForOperationType.addMeasurement(interleaveDurationAsMilli);
            }
            previousOperationStartTimesAsMilliByOperationType.put(operationType, operationStartTimeAsMilli);

            // Dependency duration by operation type
            long lowestDependencyDurationAsMilliForOperationType = (lowestDependencyDurationAsMilliByOperationType.containsKey(operationType))
                    ? lowestDependencyDurationAsMilliByOperationType.get(operationType)
                    : Long.MAX_VALUE;
            if (operationDependencyDurationAsMilli < lowestDependencyDurationAsMilliForOperationType)
                lowestDependencyDurationAsMilliByOperationType.put(operationType, operationDependencyDurationAsMilli);

            // First start times by operation type
            if (false == firstStartTimesAsMilliByOperationType.containsKey(operationType))
                firstStartTimesAsMilliByOperationType.put(operationType, operationStartTimeAsMilli);

            // Last start times by operation type
            lastStartTimesAsMilliByOperationType.put(operationType, operationStartTimeAsMilli);
        }

        return new WorkloadStatistics(
                firstStartTimesAsMilliByOperationType,
                lastStartTimesAsMilliByOperationType,
                operationMixHistogram,
                operationInterleaves,
                interleavesForDependencyOperations,
                interleavesForDependentOperations,
                operationInterleavesByOperationType,
                dependencyOperationTypes,
                dependentOperationTypes,
                lowestDependencyDurationAsMilliByOperationType);
    }
}
