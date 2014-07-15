package com.ldbc.driver;

import com.ldbc.driver.temporal.Time;

public abstract class Operation<RESULT_TYPE> {
    private Time scheduledStartTime = null;
    private Time dependencyTime = null;

    public final void setScheduledStartTime(Time scheduledStartTime) {
        this.scheduledStartTime = scheduledStartTime;
    }

    public final void setDependencyTime(Time dependencyTime) {
        this.dependencyTime = dependencyTime;
    }

    public final Time scheduledStartTime() {
        return scheduledStartTime;
    }

    public final Time dependencyTime() {
        return dependencyTime;
    }

    public final OperationResultReport buildResult(int resultCode, RESULT_TYPE result) {
        return new OperationResultReport(resultCode, result);
    }

    public String type() {
        return getClass().getName();
    }

    @Override
    public String toString() {
        return "Operation{" +
                "scheduledStartTime=" + scheduledStartTime +
                ", dependencyTime=" + dependencyTime +
                ", type=" + type() +
                '}';
    }

    public abstract RESULT_TYPE marshalResult(String serializedOperationResult) throws SerializingMarshallingException;

    public abstract String serializeResult(Object operationResultInstance) throws SerializingMarshallingException;
}
