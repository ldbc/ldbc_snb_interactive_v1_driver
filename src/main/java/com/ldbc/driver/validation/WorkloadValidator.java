package com.ldbc.driver.validation;

import com.google.common.collect.Sets;
import com.ldbc.driver.*;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.ldbc.driver.validation.WorkloadValidationResult.ResultType;

// TODO add check that all ExecutionMode:GctMode combinations make sense (e.g., Partial+GctNone does not make sense unless window size can somehow be specified)
// TODO the below could be used as a guide for how to do this
// Synchronous      NONE    makesSense(y)    dependencyTime(n)  startTimeDependencyTimeDifference(n)
// Asynchronous     NONE    makesSense(y)    dependencyTime(n)  startTimeDependencyTimeDifference(n)
// Windowed         NONE    makesSense(n)
//
// Synchronous      READ    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
// Asynchronous     READ    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
// Windowed         READ    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=windowDuration)
//
// Synchronous      READ_WRITE    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
// Asynchronous     READ_WRITE    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=0)
// Windowed         READ_WRITE    makesSense(y)    dependencyTime(y)  startTimeDependencyTimeDifference(>=windowDuration)

// TODO add test for ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_SCHEDULING_MODE
public class WorkloadValidator {
    public static final Duration DEFAULT_MAX_EXPECTED_INTERLEAVE = Duration.fromMinutes(30);

    /**
     * @param workload      must have already been initialized
     * @param configuration
     * @return
     */
    public WorkloadValidationResult validate(Workload workload, DriverConfiguration configuration) {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        Iterator<Operation<?>> operations;
        try {
            operations = workload.operations(gf, configuration.operationCount());
        } catch (WorkloadException e) {
            return new WorkloadValidationResult(ResultType.UNEXPECTED,
                    String.format("Error while retrieving operations from workload\n%s",
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }

        Map<Class<? extends Operation>, OperationClassification> operationClassifications;
        try {
            operationClassifications = workload.operationClassifications();
        } catch (WorkloadException e) {
            return new WorkloadValidationResult(ResultType.UNEXPECTED,
                    String.format("Error while retrieving operation classifications from workload\n%s",
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }

        Set<Class<? extends Operation>> operationsTypesWithClassifications = Sets.newHashSet(operationClassifications.keySet());

        Operation<?> previousOperation = null;
        Time previousOperationStartTime = null;

        Map<OperationClassification.GctMode, Time> previousOperationStartTimesByGctMode = new HashMap<>();
        Map<OperationClassification.GctMode, ContinuousMetricManager> operationInterleavesByGctMode = new HashMap<>();

        Map<Class, Time> previousOperationStartTimesByOperationType = new HashMap<>();
        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            Class operationType = operation.getClass();

            // Remove all encountered operation types to check if any classifications exist operations that do not appear in workload
            operationsTypesWithClassifications.remove(operationType);

            // Operation has start time
            Time operationStartTime = operation.scheduledStartTime();
            if (null == operationStartTime) {
                return new WorkloadValidationResult(
                        ResultType.UNASSIGNED_SCHEDULED_START_TIME,
                        String.format("Unassigned operation scheduled start time\n  %s",
                                operation));
            }

            // Operation start times increase monotonically
            if (null != previousOperationStartTime) {
                if (operationStartTime.lt(previousOperationStartTime))
                    return new WorkloadValidationResult(
                            ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY,
                            String.format(""
                                            + "Operation start times do not increase monotonically\n"
                                            + "  Previous: %s\n"
                                            + "  Current: %s",
                                    previousOperation,
                                    operation));
            }

            // Interleaves do not exceed maximum
            if (null != previousOperationStartTime) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTime);
                if (interleaveDuration.gt(workload.maxExpectedInterleave()))
                    return new WorkloadValidationResult(
                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM,
                            String.format(""
                                            + "Encountered interleave duration (%s) exceeds maximum expected interleave (%s)\n"
                                            + "  Previous: %s\n"
                                            + "  Current: %s",
                                    interleaveDuration,
                                    workload.maxExpectedInterleave(),
                                    previousOperation,
                                    operation));
            }

            // Operation has classification
            OperationClassification operationClassification = operationClassifications.get(operationType);
            if (null == operationClassification) {
                return new WorkloadValidationResult(
                        ResultType.OPERATION_HAS_NO_CLASSIFICATION,
                        String.format("Operation has no classification\n  %s", operation));
            }

            // Classification has GCT mode
            OperationClassification.GctMode operationGctMode = operationClassification.gctMode();
            if (null == operationGctMode) {
                return new WorkloadValidationResult(
                        ResultType.OPERATION_CLASSIFICATION_HAS_NO_GCT_MODE,
                        String.format("Operation has no GCT mode\nOperation: %s\nClassification: %s",
                                operation,
                                operationClassification));
            }

            // Classification has scheduling mode
            OperationClassification.SchedulingMode operationSchedulingMode = operationClassification.schedulingMode();
            if (null == operationSchedulingMode) {
                return new WorkloadValidationResult(
                        ResultType.OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE,
                        String.format("Operation has no scheduling mode\nOperation: %s\nClassification: %s",
                                operation,
                                operationClassification));
            }

            Time operationDependencyTime = operation.dependencyTime();
            // Operations with GCT mode NONE do not need a dependency time because they have no dependencies
            if (false == operationGctMode.equals(OperationClassification.GctMode.NONE)) {
                // Operation has dependency time
                if (null == operationDependencyTime) {
                    return new WorkloadValidationResult(
                            ResultType.UNASSIGNED_DEPENDENCY_TIME,
                            String.format("Unassigned operation dependency time\nOperation: %s",
                                    operation));
                }

                // Operation dependency time is less than or equal to operation start time
                if (operationDependencyTime.gt(operationStartTime)) {
                    return new WorkloadValidationResult(
                            ResultType.DEPENDENCY_TIME_IS_LATER_THAN_SCHEDULED_START_TIME,
                            String.format(""
                                            + "Operation dependency time is later than operation start time\n"
                                            + "  Operation: %s\n"
                                            + "  Start Time: %s\n"
                                            + "  Dependency Time: %s",
                                    operation,
                                    operation.scheduledStartTime(),
                                    operation.dependencyTime()));
                }

                // Duration between start time and dependency time should be at least Window Duration for operations with Windowed scheduling mode
                if (operationSchedulingMode.equals(OperationClassification.SchedulingMode.WINDOWED) &&
                        operationStartTime.greaterBy(operationDependencyTime).lt(configuration.windowedExecutionWindowDuration())) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("Duration between scheduled start time & dependency time is insufficient for an operation with ").append(OperationClassification.SchedulingMode.WINDOWED.name()).append(" scheduling mode\n");
                    sb.append("-> should be greater or equal to 'window duration'\n");
                    sb.append("Operation: ").append(operation.toString()).append("\n");
                    sb.append("Start Time: ").append(operationStartTime).append("\n");
                    sb.append("Dependency Time: ").append(operationDependencyTime).append("\n");
                    sb.append("Actual Duration: ").append(operationStartTime.greaterBy(operationDependencyTime)).append("\n");
                    sb.append("Window Duration: ").append(configuration.windowedExecutionWindowDuration()).append("\n");
                    return new WorkloadValidationResult(
                            ResultType.INSUFFICIENT_INTERVAL_BETWEEN_DEPENDENCY_TIME_AND_SCHEDULED_START_TIME,
                            sb.toString());
                }
            }

            // Interleaves by GCT mode do not exceed maximum
            ContinuousMetricManager operationInterleaveForGctMode = operationInterleavesByGctMode.get(operationGctMode);
            if (null == operationInterleaveForGctMode) {
                operationInterleaveForGctMode = new ContinuousMetricManager(null, null, workload.maxExpectedInterleave().asMilli(), 5);
                operationInterleavesByGctMode.put(operationGctMode, operationInterleaveForGctMode);
            }
            Time previousOperationStartTimeByGctMode = previousOperationStartTimesByGctMode.get(operationGctMode);
            if (null != previousOperationStartTimeByGctMode) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTimeByGctMode);
                if (interleaveDuration.gt(workload.maxExpectedInterleave()))
                    return new WorkloadValidationResult(
                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_GCT_MODE,
                            String.format("Encountered (for %s GCT mode) interleave duration (%s) that exceeds maximum expected value (%s)",
                                    operationGctMode,
                                    interleaveDuration,
                                    workload.maxExpectedInterleave()));
            }

            // Interleaves by operation type do not exceed maximum
            ContinuousMetricManager operationInterleaveForOperationType = operationInterleavesByOperationType.get(operationType);
            if (null == operationInterleaveForOperationType) {
                operationInterleaveForOperationType = new ContinuousMetricManager(null, null, workload.maxExpectedInterleave().asMilli(), 5);
                operationInterleavesByOperationType.put(operationType, operationInterleaveForOperationType);
            }
            Time previousOperationStartTimeByOperationType = previousOperationStartTimesByOperationType.get(operationType);
            if (null != previousOperationStartTimeByOperationType) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTimeByOperationType);
                if (interleaveDuration.gt(workload.maxExpectedInterleave()))
                    return new WorkloadValidationResult(
                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_OPERATION_TYPE,
                            String.format(""
                                            + "Encountered interleave (for %s type) duration (%s) that exceeds maximum expected value (%s)\n"
                                            + "  Previous: %s\n"
                                            + "  Current: %s",
                                    operationType.getSimpleName(),
                                    interleaveDuration,
                                    workload.maxExpectedInterleave(),
                                    previousOperationStartTimeByOperationType,
                                    operationStartTime));
            }

            // Serializing and Marshalling operations works
            String serializedOperation;
            try {
                serializedOperation = workload.serializeOperation(operation);
            } catch (SerializingMarshallingException e) {
                return new WorkloadValidationResult(
                        ResultType.UNABLE_TO_SERIALIZE_OPERATION,
                        String.format("Unable to serialize operation\nOperation: %s",
                                operation));
            }
            Operation<?> marshaledOperation;
            try {
                marshaledOperation = workload.marshalOperation(serializedOperation);
            } catch (SerializingMarshallingException e) {
                return new WorkloadValidationResult(
                        ResultType.UNABLE_TO_MARSHAL_OPERATION,
                        String.format("Unable to marshal operation\nOperation: %s",
                                serializedOperation));
            }
            if (false == operation.equals(marshaledOperation)) {
                return new WorkloadValidationResult(
                        ResultType.OPERATIONS_DO_NOT_EQUAL_AFTER_SERIALIZING_AND_MARSHALLING,
                        String.format(""
                                        + "Operations do not equal after serializing and marshalling\n"
                                        + "  Original Operation: %s\n"
                                        + "  Serialized Operation: %s\n"
                                        + "  Marshaled Operation: %s",
                                operation,
                                serializedOperation,
                                marshaledOperation));
            }

            previousOperation = operation;
            previousOperationStartTime = operationStartTime;
            previousOperationStartTimesByGctMode.put(operationGctMode, operationStartTime);
            previousOperationStartTimesByOperationType.put(operationType, operationStartTime);
        }

        if (false == operationsTypesWithClassifications.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("Operation types have classifications but were not generated by the workload\n");
            for (Class opType : operationsTypesWithClassifications)
                sb.append("  ").append(opType.getSimpleName()).append("\n");
            return new WorkloadValidationResult(
                    ResultType.OPERATION_TYPES_HAVE_CLASSIFICATIONS_BUT_WERE_NOT_GENERATED,
                    sb.toString());
        }

        Iterator<Operation<?>> operationStream1;
        Iterator<Operation<?>> operationStream2;
        try {
            operationStream1 = workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42l)), configuration.operationCount());
            operationStream2 = workload.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42l)), configuration.operationCount());
        } catch (WorkloadException e) {
            return new WorkloadValidationResult(
                    ResultType.UNEXPECTED,
                    String.format("Error while retrieving operations workload\n%s",
                            ConcurrentErrorReporter.stackTraceToString(e)));
        }

        boolean compareTimes = true;
        if (false == gf.compareOperationStreams(operationStream1, operationStream2, compareTimes)) {
            return new WorkloadValidationResult(
                    ResultType.WORKLOAD_IS_NOT_DETERMINISTIC,
                    "Workload is not deterministic");
        }

        return new WorkloadValidationResult(ResultType.SUCCESSFUL, null);
    }
}
