package com.ldbc.driver;

import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;

public class OperationResultReport {
    private final int resultCode;
    private final Object operationResult;

    private Time scheduledStartTime = null;
    private Time actualStartTime = null;
    private Duration runDuration = null;
    private String operationType = null;

    OperationResultReport(int resultCode, Object operationResult) {
        this.resultCode = resultCode;
        this.operationResult = operationResult;
    }

    public int resultCode() {
        return resultCode;
    }

    public Object operationResult() {
        return operationResult;
    }

    public Time scheduledStartTime() {
        return scheduledStartTime;
    }

    void setScheduledStartTime(Time scheduledStartTime) {
        this.scheduledStartTime = scheduledStartTime;
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

    public String operationType() {
        return operationType;
    }

    void setOperationType(String operationType) {
        this.operationType = operationType;
    }

    @Override
    public String toString() {
        return "OperationResultReport [resultCode=" + resultCode + ", operationResult=" + operationResult + ", scheduledStartTime="
                + scheduledStartTime + ", actualStartTime=" + actualStartTime + ", runDuration=" + runDuration
                + ", operationType=" + operationType + "]";
    }
}
