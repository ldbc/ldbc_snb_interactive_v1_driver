package com.ldbc.driver.validation;

import com.google.common.collect.Sets;
import com.ldbc.driver.*;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;

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
    /**
     * @param workloadFactory
     * @param configuration
     * @return
     */
    public WorkloadValidationResult validate(WorkloadFactory workloadFactory, DriverConfiguration configuration) {
//        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
//        TimeSource timeSource = new SystemTimeSource();
//        Time now = timeSource.now();
//        long operationCount;
//
//        /*
//         * *************************************************************************************************************
//         *   FIRST PHASE JUST CHECK THAT ALL OPERATIONS HAVE TIMES ASSIGNED
//         * *************************************************************************************************************
//         */
//        Workload workloadPass1;
//        try {
//            workloadPass1 = workloadFactory.createWorkload();
//        } catch (ValidationException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload creation\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//        try {
//            workloadPass1.init(configuration);
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(ResultType.UNEXPECTED,
//                    String.format("Error while initializing workload\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//        Iterator<Operation<?>> operationsPass1;
//        try {
//            operationsPass1 = workloadPass1.operations(gf, configuration.operationCount());
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(ResultType.UNEXPECTED,
//                    String.format("Error while retrieving operations from workload\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//        operationCount = 0;
//        while (operationsPass1.hasNext()) {
//            Operation<?> operation = operationsPass1.next();
//            operationCount++;
//
//            // Operation has start time
//            Time operationStartTime = operation.scheduledStartTime();
//            if (null == operationStartTime) {
//                return new WorkloadValidationResult(
//                        ResultType.UNASSIGNED_SCHEDULED_START_TIME,
//                        String.format("Operation %s - Unassigned operation scheduled start time\n  %s",
//                                operationCount,
//                                operation));
//            }
//
//            Time operationDependencyTime = operation.dependencyTime();
//            // Operation has dependency time
//            if (null == operationDependencyTime) {
//                return new WorkloadValidationResult(
//                        ResultType.UNASSIGNED_DEPENDENCY_TIME,
//                        String.format("Operation %s - Unassigned operation dependency time\nOperation: %s",
//                                operationCount,
//                                operation));
//            }
//
//            // Ensure operation dependency time is less than operation start time
//            if (false == operationDependencyTime.lt(operationStartTime)) {
//                return new WorkloadValidationResult(
//                        ResultType.DEPENDENCY_TIME_IS_NOT_BEFORE_SCHEDULED_START_TIME,
//                        String.format(""
//                                        + "Operation %s - Operation dependency time is not less than operation start time\n"
//                                        + "  Operation: %s\n"
//                                        + "  Start Time: %s\n"
//                                        + "  Dependency Time: %s",
//                                operationCount,
//                                operation,
//                                operation.scheduledStartTime(),
//                                operation.dependencyTime()));
//            }
//        }
//
//        try {
//            workloadPass1.cleanup();
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload cleanup\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//
//        /*
//         * *************************************************************************************************************
//         *   SECOND PHASE PERFORM MORE ELABORATE CHECKS
//         * *************************************************************************************************************
//         */
//
//        Workload workloadPass2;
//        try {
//            workloadPass2 = workloadFactory.createWorkload();
//        } catch (ValidationException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload creation\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//        try {
//            workloadPass2.init(configuration);
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(ResultType.UNEXPECTED,
//                    String.format("Error while initializing workload\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//
//        Iterator<Operation<?>> operationsPass2;
//        try {
//            operationsPass2 = gf.timeOffsetAndCompress(
//                    workloadPass2.operations(
//                            gf,
//                            configuration.operationCount()
//                    ),
//                    now,
//                    configuration.timeCompressionRatio()
//            );
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(ResultType.UNEXPECTED,
//                    String.format("Error while retrieving operations from workload\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//        Map<Class<? extends Operation>, OperationClassification> operationClassifications;
//        try {
//            operationClassifications = workloadPass2.operationClassifications();
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(ResultType.UNEXPECTED,
//                    String.format("Error while retrieving operation classifications from workload\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//        Set<Class<? extends Operation>> operationsTypesWithClassifications = Sets.newHashSet(operationClassifications.keySet());
//
//        Operation<?> previousOperation = null;
//        Time previousOperationStartTime = null;
//
//        Map<OperationClassification.DependencyMode, Time> previousOperationStartTimesByGctMode = new HashMap<>();
//        Map<OperationClassification.DependencyMode, ContinuousMetricManager> operationInterleavesByGctMode = new HashMap<>();
//
//        Map<Class, Time> previousOperationStartTimesByOperationType = new HashMap<>();
//        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();
//
//        operationCount = 0;
//        while (operationsPass2.hasNext()) {
//            Operation<?> operation = operationsPass2.next();
//            Class operationType = operation.getClass();
//            operationCount++;
//
//            // Remove all encountered operation types to check if any classifications exist operations that do not appear in workload
//            operationsTypesWithClassifications.remove(operationType);
//
//            // Operation has start time
//            Time operationStartTime = operation.scheduledStartTime();
//            if (null == operationStartTime) {
//                return new WorkloadValidationResult(
//                        ResultType.UNASSIGNED_SCHEDULED_START_TIME,
//                        String.format("Operation %s - Unassigned operation scheduled start time\n  %s",
//                                operationCount,
//                                operation));
//            }
//
//            // Operation start times increase monotonically
//            if (null != previousOperationStartTime) {
//                if (operationStartTime.lt(previousOperationStartTime))
//                    return new WorkloadValidationResult(
//                            ResultType.SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY,
//                            String.format(""
//                                            + "Operation %s - Operation start times do not increase monotonically\n"
//                                            + "  Previous: %s\n"
//                                            + "  Current: %s",
//                                    operationCount,
//                                    previousOperation,
//                                    operation));
//            }
//
//            // Interleaves do not exceed maximum
//            if (null != previousOperationStartTime) {
//                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTime);
//                if (interleaveDuration.gt(workloadPass2.maxExpectedInterleave()))
//                    return new WorkloadValidationResult(
//                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM,
//                            String.format(""
//                                            + "Operation %s - Encountered interleave duration (%s) exceeds maximum expected interleave (%s)\n"
//                                            + "  Previous: %s\n"
//                                            + "  Current: %s",
//                                    operationCount,
//                                    interleaveDuration,
//                                    workloadPass2.maxExpectedInterleave(),
//                                    previousOperation,
//                                    operation));
//            }
//
//            // Operation has classification
//            OperationClassification operationClassification = operationClassifications.get(operationType);
//            if (null == operationClassification) {
//                return new WorkloadValidationResult(
//                        ResultType.OPERATION_HAS_NO_CLASSIFICATION,
//                        String.format("Operation %s - Operation has no classification\n  %s",
//                                operationCount,
//                                operation));
//            }
//
//            // Classification has GCT mode
//            OperationClassification.DependencyMode operationDependencyMode = operationClassification.dependencyMode();
//            if (null == operationDependencyMode) {
//                return new WorkloadValidationResult(
//                        ResultType.OPERATION_CLASSIFICATION_HAS_NO_GCT_MODE,
//                        String.format("Operation %s - Operation has no GCT mode\nOperation: %s\nClassification: %s",
//                                operationCount,
//                                operation,
//                                operationClassification));
//            }
//
//            // Classification has scheduling mode
//            OperationClassification.SchedulingMode operationSchedulingMode = operationClassification.schedulingMode();
//            if (null == operationSchedulingMode) {
//                return new WorkloadValidationResult(
//                        ResultType.OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE,
//                        String.format("Operation %s - Operation has no scheduling mode\nOperation: %s\nClassification: %s",
//                                operationCount,
//                                operation,
//                                operationClassification));
//            }
//
//            Time operationDependencyTime = operation.dependencyTime();
//            // Operation has dependency time
//            if (null == operationDependencyTime) {
//                return new WorkloadValidationResult(
//                        ResultType.UNASSIGNED_DEPENDENCY_TIME,
//                        String.format("Operation %s - Unassigned operation dependency time\nOperation: %s",
//                                operationCount,
//                                operation));
//            }
//
//            // Ensure operation dependency time is less than operation start time
//            if (false == operationDependencyTime.lt(operationStartTime)) {
//                return new WorkloadValidationResult(
//                        ResultType.DEPENDENCY_TIME_IS_NOT_BEFORE_SCHEDULED_START_TIME,
//                        String.format(""
//                                        + "Operation %s - Operation dependency time is not less than operation start time\n"
//                                        + "  Operation: %s\n"
//                                        + "  Start Time: %s\n"
//                                        + "  Dependency Time: %s",
//                                operationCount,
//                                operation,
//                                operation.scheduledStartTime(),
//                                operation.dependencyTime()));
//            }
//
//            // Duration between start time and dependency time should be at least Window Duration for operations with Windowed scheduling mode
//            if (operationSchedulingMode.equals(OperationClassification.SchedulingMode.WINDOWED) &&
//                    operationStartTime.durationGreaterThan(operationDependencyTime).lt(configuration.windowedExecutionWindowDuration())) {
//                StringBuilder sb = new StringBuilder();
//                sb.append("Operation ").append(operationCount).append(" - Duration between scheduled start time & dependency time is insufficient for an operation with ").append(OperationClassification.SchedulingMode.WINDOWED.name()).append(" scheduling mode\n");
//                sb.append("-> should be greater or equal to 'window duration'\n");
//                sb.append("Operation: ").append(operation.toString()).append("\n");
//                sb.append("Start Time: ").append(operationStartTime).append("\n");
//                sb.append("Dependency Time: ").append(operationDependencyTime).append("\n");
//                sb.append("Actual Duration: ").append(operationStartTime.durationGreaterThan(operationDependencyTime)).append("\n");
//                sb.append("Window Duration: ").append(configuration.windowedExecutionWindowDuration()).append("\n");
//                return new WorkloadValidationResult(
//                        ResultType.INSUFFICIENT_INTERVAL_BETWEEN_DEPENDENCY_TIME_AND_SCHEDULED_START_TIME,
//                        sb.toString());
//            }
//
//            // Interleaves by GCT mode do not exceed maximum
//            ContinuousMetricManager operationInterleaveForGctMode = operationInterleavesByGctMode.get(operationDependencyMode);
//            if (null == operationInterleaveForGctMode) {
//                operationInterleaveForGctMode = new ContinuousMetricManager(null, null, workloadPass2.maxExpectedInterleave().asMilli(), 5);
//                operationInterleavesByGctMode.put(operationDependencyMode, operationInterleaveForGctMode);
//            }
//            Time previousOperationStartTimeByGctMode = previousOperationStartTimesByGctMode.get(operationDependencyMode);
//            if (null != previousOperationStartTimeByGctMode) {
//                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTimeByGctMode);
//                if (interleaveDuration.gt(workloadPass2.maxExpectedInterleave()))
//                    return new WorkloadValidationResult(
//                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_GCT_MODE,
//                            String.format("Operation %s - Encountered (for %s GCT mode) interleave duration (%s) that exceeds maximum expected value (%s)",
//                                    operationCount,
//                                    operationDependencyMode,
//                                    interleaveDuration,
//                                    workloadPass2.maxExpectedInterleave()));
//            }
//
//            // Interleaves by operation type do not exceed maximum
//            ContinuousMetricManager operationInterleaveForOperationType = operationInterleavesByOperationType.get(operationType);
//            if (null == operationInterleaveForOperationType) {
//                operationInterleaveForOperationType = new ContinuousMetricManager(null, null, workloadPass2.maxExpectedInterleave().asMilli(), 5);
//                operationInterleavesByOperationType.put(operationType, operationInterleaveForOperationType);
//            }
//            Time previousOperationStartTimeByOperationType = previousOperationStartTimesByOperationType.get(operationType);
//            if (null != previousOperationStartTimeByOperationType) {
//                Duration interleaveDuration = operationStartTime.durationGreaterThan(previousOperationStartTimeByOperationType);
//                if (interleaveDuration.gt(workloadPass2.maxExpectedInterleave()))
//                    return new WorkloadValidationResult(
//                            ResultType.SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_OPERATION_TYPE,
//                            String.format(""
//                                            + "Operation %s - Encountered interleave duration (for %s) %s that exceeds maximum expected value (%s)\n"
//                                            + "  Previous: %s\n"
//                                            + "  Current: %s",
//                                    operationCount,
//                                    operationType.getSimpleName(),
//                                    interleaveDuration,
//                                    workloadPass2.maxExpectedInterleave(),
//                                    previousOperationStartTimeByOperationType,
//                                    operationStartTime));
//            }
//
//            // Serializing and Marshalling operations works
//            String serializedOperation;
//            try {
//                serializedOperation = workloadPass2.serializeOperation(operation);
//            } catch (SerializingMarshallingException e) {
//                return new WorkloadValidationResult(
//                        ResultType.UNABLE_TO_SERIALIZE_OPERATION,
//                        String.format("Operation %s - Unable to serialize operation\nOperation: %s",
//                                operationCount,
//                                operation));
//            }
//            Operation<?> marshaledOperation;
//            try {
//                marshaledOperation = workloadPass2.marshalOperation(serializedOperation);
//            } catch (SerializingMarshallingException e) {
//                return new WorkloadValidationResult(
//                        ResultType.UNABLE_TO_MARSHAL_OPERATION,
//                        String.format("Unable to marshal operation\nOperation: %s",
//                                serializedOperation));
//            }
//            if (false == operation.equals(marshaledOperation)) {
//                return new WorkloadValidationResult(
//                        ResultType.OPERATIONS_DO_NOT_EQUAL_AFTER_SERIALIZING_AND_MARSHALLING,
//                        String.format(""
//                                        + "Operation %s - Operations do not equal after serializing and marshalling\n"
//                                        + "  Original Operation: %s\n"
//                                        + "  Serialized Operation: %s\n"
//                                        + "  Marshaled Operation: %s",
//                                operationCount,
//                                operation,
//                                serializedOperation,
//                                marshaledOperation));
//            }
//
//            previousOperation = operation;
//            previousOperationStartTime = operationStartTime;
//            previousOperationStartTimesByGctMode.put(operationDependencyMode, operationStartTime);
//            previousOperationStartTimesByOperationType.put(operationType, operationStartTime);
//        }
//
//        if (false == operationsTypesWithClassifications.isEmpty()) {
//            StringBuilder sb = new StringBuilder();
//            sb.append("Operation ").append(operationCount).append(" - Operation types have classifications but were not generated by the workload\n");
//            for (Class opType : operationsTypesWithClassifications) {
//                sb.append("  ").append(opType.getSimpleName()).append("\n");
//            }
//            return new WorkloadValidationResult(
//                    ResultType.OPERATION_TYPES_HAVE_CLASSIFICATIONS_BUT_WERE_NOT_GENERATED,
//                    sb.toString());
//        }
//
//        try {
//            workloadPass2.cleanup();
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload cleanup\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//
//        /*
//         * *************************************************************************************************************
//         *   THIRD PHASE PERFORM DETERMINISM CHECK
//         * *************************************************************************************************************
//         */
//
//        Workload workload1;
//        Workload workload2;
//        try {
//            workload1 = workloadFactory.createWorkload();
//            workload2 = workloadFactory.createWorkload();
//        } catch (ValidationException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload creation\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//
//        try {
//            workload1.init(configuration);
//            workload2.init(configuration);
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload initialization\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//
//        Iterator<Operation<?>> operationStream1;
//        Iterator<Operation<?>> operationStream2;
//        try {
//            operationStream1 = gf.timeOffset(
//                    workload1.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42l)), configuration.operationCount()),
//                    now
//            );
//            operationStream2 = gf.timeOffset(
//                    workload2.operations(new GeneratorFactory(new RandomDataGeneratorFactory(42l)), configuration.operationCount()),
//                    now
//            );
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    String.format("Error while retrieving operations workload\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//        try {
//            boolean compareTimes = true;
//            GeneratorFactory.OperationStreamComparisonResult operationStreamComparisonResult =
//                    gf.compareOperationStreams(operationStream1, operationStream2, compareTimes);
//            if (GeneratorFactory.OperationStreamComparisonResultType.PASS != operationStreamComparisonResult.resultType()) {
//                return new WorkloadValidationResult(
//                        ResultType.WORKLOAD_IS_NOT_DETERMINISTIC,
//                        "Workload is not deterministic\n" + operationStreamComparisonResult.errorMessage());
//            }
//        } catch (Exception e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    String.format("Unexpected error encountered while checking if workload is deterministic\n%s",
//                            ConcurrentErrorReporter.stackTraceToString(e)));
//        }
//
//        try {
//            workload1.cleanup();
//            workload2.cleanup();
//        } catch (WorkloadException e) {
//            return new WorkloadValidationResult(
//                    ResultType.UNEXPECTED,
//                    "Error during workload creation\n" + ConcurrentErrorReporter.stackTraceToString(e));
//        }
//
//        return new WorkloadValidationResult(ResultType.SUCCESSFUL, null);
        throw new RuntimeException("Uncomment and re-implement");
    }
}
