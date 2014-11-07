package com.ldbc.driver;

import com.ldbc.driver.temporal.TemporalUtil;

public abstract class Operation<RESULT_TYPE> {
    private static final TemporalUtil temporalutil = new TemporalUtil();
    private long scheduledStartTimeAsMilli = -1;
    private long timeStamp = -1;
    private long dependencyTimeStamp = -1;

    public final void setScheduledStartTimeAsMilli(long scheduledStartTimeAsMilli) {
        this.scheduledStartTimeAsMilli = scheduledStartTimeAsMilli;
    }

    public final void setDependencyTimeStamp(long dependencyTimeStamp) {
        this.dependencyTimeStamp = dependencyTimeStamp;
    }

    public final long scheduledStartTimeAsMilli() {
        return scheduledStartTimeAsMilli;
    }

    public final long dependencyTimeStamp() {
        return dependencyTimeStamp;
    }

    public long timeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public final OperationResultReport buildResult(int resultCode, RESULT_TYPE result) {
        return new OperationResultReport(resultCode, result, this);
    }

    public String type() {
        return getClass().getName();
    }

    @Override
    public String toString() {
        return "Operation{" +
                "scheduledStartTime=" + temporalutil.millisecondsToDateTimeString(scheduledStartTimeAsMilli) +
                ", timeStamp=" + temporalutil.millisecondsToDateTimeString(timeStamp) +
                ", dependencyTimeStamp=" + temporalutil.millisecondsToDateTimeString(dependencyTimeStamp) +
                '}';
    }

    public abstract RESULT_TYPE marshalResult(String serializedOperationResult) throws SerializingMarshallingException;

    public abstract String serializeResult(Object operationResultInstance) throws SerializingMarshallingException;
}
