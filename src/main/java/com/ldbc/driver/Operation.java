package com.ldbc.driver;

import com.ldbc.driver.temporal.TemporalUtil;

public abstract class Operation<RESULT_TYPE> {
    private static final TemporalUtil temporalutil = new TemporalUtil();
    private long scheduledStartTimeAsMilli = -1;
    private long dependencyTimeAsMilli = -1;

    public final void setScheduledStartTimeAsMilli(long scheduledStartTimeAsMilli) {
        this.scheduledStartTimeAsMilli = scheduledStartTimeAsMilli;
    }

    public final void setDependencyTimeAsMilli(long dependencyTimeAsMilli) {
        this.dependencyTimeAsMilli = dependencyTimeAsMilli;
    }

    public final long scheduledStartTimeAsMilli() {
        return scheduledStartTimeAsMilli;
    }

    public final long dependencyTimeAsMilli() {
        return dependencyTimeAsMilli;
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
                "scheduledStartTimeAsMilli=" + scheduledStartTimeAsMilli +
                ", scheduledStartTime=" + temporalutil.millisecondsToTimeString(scheduledStartTimeAsMilli) +
                ", dependencyTimeAsMilli=" + dependencyTimeAsMilli +
                ", dependencyTime=" + temporalutil.millisecondsToTimeString(dependencyTimeAsMilli) +
                '}';
    }

    public abstract RESULT_TYPE marshalResult(String serializedOperationResult) throws SerializingMarshallingException;

    public abstract String serializeResult(Object operationResultInstance) throws SerializingMarshallingException;
}
