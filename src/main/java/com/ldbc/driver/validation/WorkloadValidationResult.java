package com.ldbc.driver.validation;

public class WorkloadValidationResult {
    public enum ResultType {
        SUCCESSFUL,
        UNEXPECTED,
        UNASSIGNED_SCHEDULED_START_TIME,
        SCHEDULED_START_TIMES_DO_NOT_INCREASE_MONOTONICALLY,
        SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM,
        OPERATION_HAS_NO_CLASSIFICATION,
        OPERATION_CLASSIFICATION_HAS_NO_GCT_MODE,
        OPERATION_CLASSIFICATION_HAS_NO_SCHEDULING_MODE,
        UNASSIGNED_DEPENDENCY_TIME,
        DEPENDENCY_TIME_IS_NOT_BEFORE_SCHEDULED_START_TIME,
        INSUFFICIENT_INTERVAL_BETWEEN_DEPENDENCY_TIME_AND_SCHEDULED_START_TIME,
        SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_GCT_MODE,
        SCHEDULED_START_TIME_INTERVAL_EXCEEDS_MAXIMUM_FOR_OPERATION_TYPE,
        UNABLE_TO_SERIALIZE_OPERATION,
        UNABLE_TO_MARSHAL_OPERATION,
        OPERATIONS_DO_NOT_EQUAL_AFTER_SERIALIZING_AND_MARSHALLING,
        OPERATION_TYPES_HAVE_CLASSIFICATIONS_BUT_WERE_NOT_GENERATED,
        WORKLOAD_IS_NOT_DETERMINISTIC
    }

    private final ResultType resultType;
    private final String errorMessage;

    public WorkloadValidationResult(ResultType resultType, String errorMessage) {
        this.errorMessage = errorMessage;
        this.resultType = resultType;
    }

    public boolean isSuccessful() {
        return resultType.equals(ResultType.SUCCESSFUL);
    }

    public ResultType resultType() {
        return resultType;
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public String toString() {
        return "WorkloadValidationResult{" +
                "resultType=" + resultType +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
