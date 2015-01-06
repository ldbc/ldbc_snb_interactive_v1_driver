package com.ldbc.driver;

import com.ldbc.driver.temporal.TemporalUtil;

public class OperationResultReport {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private final int resultCode;
    private final Object operationResult;
    private final Operation<?> operation;

    private long actualStartTimeAsMilli = -1;
    private long runDurationAsNano = -1;

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

    public long actualStartTimeAsMilli() {
        return actualStartTimeAsMilli;
    }

    void setActualStartTimeAsMilli(long actualStartTimeAsMilli) {
        this.actualStartTimeAsMilli = actualStartTimeAsMilli;
    }

    public long runDurationAsNano() {
        return runDurationAsNano;
    }

    void setRunDurationAsNano(long runDurationAsNano) {
        this.runDurationAsNano = runDurationAsNano;
    }

    public Operation<?> operation() {
        return operation;
    }

    @Override
    public String toString() {
        return "OperationResultReport{" +
                "resultCode=" + resultCode +
                ", operationResult=" + operationResult +
                ", operation=" + operation +
                ", actualStartTimeAsMilli=" + actualStartTimeAsMilli +
                ", actualStartTime=" + TEMPORAL_UTIL.milliTimeToTimeString(actualStartTimeAsMilli) +
                ", runDurationAsNano=" + runDurationAsNano +
                ", runDuration=" + TEMPORAL_UTIL.nanoDurationToString(runDurationAsNano) +
                '}';
    }
}
