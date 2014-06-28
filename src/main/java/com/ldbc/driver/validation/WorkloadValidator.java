package com.ldbc.driver.validation;

import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.runtime.metrics.ContinuousMetricManager;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

// TODO add check that all ExecutionMode:GctMode combinations make sense (e.g., Partial+GctNone does not make sense unless window size can somehow be specified)

// TODO check that ToleratedExecutionDelay and GctMode combination makes sense.
// TODO if ToleratedExecutionDelay is too long, it will break correctness ensured by GCT.
// TODO include WindowSize in this check too.

// TODO add check that all Operations in workload can serialize/deserialize themselves correctly
// TODO check that all OperationResults in workload can serialize/deserialize themselves correctly

// TODO Workload should only return classifications for operations that are returned
// TODO Workload should return classifications for every operation that is returned

// TODO Workload should be able to serialize/marshal every operation it can generate
// TODO every generated operation should implement equals correctly
// TODO e.g. assertThat(operation,equalTo(Workload.marshal(Workload.serialize(operation)))
public class WorkloadValidator {
    public static final Duration DEFAULT_MAX_EXPECTED_INTERLEAVE = Duration.fromMinutes(30);

    public static class WorkloadValidationResult {
        private final boolean successful;
        private final String errorMessage;

        public WorkloadValidationResult(boolean successful, String errorMessage) {
            this.successful = successful;
            this.errorMessage = errorMessage;
        }

        public boolean isSuccessful() {
            return successful;
        }

        public String errorMessage() {
            return errorMessage;
        }
    }

    public WorkloadValidationResult validate(Iterator<Operation<?>> operations,
                                             Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications,
                                             Duration maxExpectedInterleave) {
        Operation<?> previousOperation = null;
        Time previousOperationStartTime = null;

        Map<OperationClassification.GctMode, Time> previousOperationStartTimesByGctMode = new HashMap<>();
        Map<OperationClassification.GctMode, ContinuousMetricManager> operationInterleavesByGctMode = new HashMap<>();

        Map<Class, Time> previousOperationStartTimesByOperationType = new HashMap<>();
        Map<Class, ContinuousMetricManager> operationInterleavesByOperationType = new HashMap<>();

        while (operations.hasNext()) {
            Operation<?> operation = operations.next();
            Class operationType = operation.getClass();

            // Operation has start time
            Time operationStartTime = operation.scheduledStartTime();
            if (null == operationStartTime)
                return new WorkloadValidationResult(
                        false,
                        String.format("Unassigned operation scheduled start time\n%s", operation));

            // Operation start times increase monotonically
            if (null != previousOperationStartTime) {
                if (operationStartTime.lt(previousOperationStartTime))
                    return new WorkloadValidationResult(
                            false,
                            String.format("Operation start times do not increase monotonically\nPrevious: %s\nCurrent: %s",
                                    previousOperation, operation));
            }

            // Interleaves do not exceed maximum
            if (null != previousOperationStartTime) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTime);
                if (interleaveDuration.gt(maxExpectedInterleave))
                    return new WorkloadValidationResult(
                            false,
                            String.format("Encountered interleave duration (%s) exceeds maximum expected interleave (%s)\nPrevious: %s\nCurrent: %s",
                                    interleaveDuration, maxExpectedInterleave, previousOperation, operation));
            }

            // Operation has classification
            OperationClassification operationClassification = operationClassifications.get(operationType);
            if (null == operationClassification)
                return new WorkloadValidationResult(false, String.format("Operation has no classification\n%s", operation));

            // Classification has GCT mode
            OperationClassification.GctMode operationGctMode = operationClassification.gctMode();
            if (null == operationGctMode)
                return new WorkloadValidationResult(false, String.format("Operation has no GCT mode\n%s", operation));

            // Classification has scheduling mode
            OperationClassification.SchedulingMode operationSchedulingMode = operationClassification.schedulingMode();
            if (null == operationSchedulingMode)
                return new WorkloadValidationResult(false, String.format("Operation has no scheduling mode\n%s", operation));

            // Interleaves by GCT mode do not exceed maximum
            ContinuousMetricManager operationInterleaveForGctMode = operationInterleavesByGctMode.get(operationGctMode);
            if (null == operationInterleaveForGctMode) {
                operationInterleaveForGctMode = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
                operationInterleavesByGctMode.put(operationGctMode, operationInterleaveForGctMode);
            }
            Time previousOperationStartTimeByGctMode = previousOperationStartTimesByGctMode.get(operationGctMode);
            if (null != previousOperationStartTimeByGctMode) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTimeByGctMode);
                if (interleaveDuration.gt(maxExpectedInterleave))
                    return new WorkloadValidationResult(
                            false,
                            String.format("Encountered (for %s GCT mode) interleave duration (%s) that exceeds maximum expected value (%s)",
                                    operationGctMode, interleaveDuration, maxExpectedInterleave));

            }

            // Interleaves by operation type do not exceed maximum
            ContinuousMetricManager operationInterleaveForOperationType = operationInterleavesByOperationType.get(operationType);
            if (null == operationInterleaveForOperationType) {
                operationInterleaveForOperationType = new ContinuousMetricManager(null, null, maxExpectedInterleave.asMilli(), 5);
                operationInterleavesByOperationType.put(operationType, operationInterleaveForOperationType);
            }
            Time previousOperationStartTimeByOperationType = previousOperationStartTimesByOperationType.get(operationType);
            if (null != previousOperationStartTimeByOperationType) {
                Duration interleaveDuration = operationStartTime.greaterBy(previousOperationStartTimeByOperationType);
                if (interleaveDuration.gt(maxExpectedInterleave))
                    return new WorkloadValidationResult(
                            false,
                            String.format("Encountered interleave (for %s type) duration (%s) that exceeds maximum expected value (%s)\nPrevious: %s\nCurrent: %s",
                                    operationType.getSimpleName(), interleaveDuration, maxExpectedInterleave, previousOperationStartTimeByOperationType, operationStartTime));
            }

            previousOperation = operation;
            previousOperationStartTime = operationStartTime;
            previousOperationStartTimesByGctMode.put(operationGctMode, operationStartTime);
            previousOperationStartTimesByOperationType.put(operationType, operationStartTime);
        }
        return new WorkloadValidationResult(true, null);
    }
}
