package com.ldbc.driver;

import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class OperationResultReport {
    private final int resultCode;
    private final Object operationResult;
    private final Operation<?> operation;

    private Time actualStartTime = null;
    private Duration runDuration = null;

    OperationResultReport(int resultCode, Object operationResult, Operation<?> operation) {
        this.resultCode = resultCode;
        this.operationResult = operationResult;
        this.operation = operation;
    }

    public int resultCode() {
        return resultCode;
    }

    public Object operationResult() {
        return operationResult;
    }

    public Time actualStartTime() {
        return actualStartTime;
    }

    void setActualStartTime(Time actualStartTime) {
        this.actualStartTime = actualStartTime;
    }

    public Duration runDuration() {
        return runDuration;
    }

    void setRunDuration(Duration runDuration) {
        this.runDuration = runDuration;
    }

    public Operation<?> operation() {
        return operation;
    }

    @Override
    public String toString() {
        return "OperationResultReport [resultCode=" + resultCode + ", operationResult=" + operationResult + ", scheduledStartTime="
                + operation.scheduledStartTime() + ", actualStartTime=" + actualStartTime + ", runDuration=" + runDuration
                + ", operationType=" + operation.type() + "]";
    }
}
