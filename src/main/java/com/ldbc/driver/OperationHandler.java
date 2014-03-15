package com.ldbc.driver;

import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.executor.Spinner;
import com.ldbc.driver.temporal.DurationMeasurement;

import java.util.concurrent.Callable;

public abstract class OperationHandler<A extends Operation<?>> implements Callable<OperationResult> {
    private Spinner spinner;
    private A operation;
    private DbConnectionState dbConnectionState;
    private ConcurrentCompletionTimeService concurrentCompletionTimeService;

    public final void init(Spinner spinner, Operation<?> operation, ConcurrentCompletionTimeService concurrentCompletionTimeService) {
        this.spinner = spinner;
        this.operation = (A) operation;
        this.concurrentCompletionTimeService = concurrentCompletionTimeService;
    }

    public final A operation() {
        return operation;
    }

    public final void setDbConnectionState(DbConnectionState dbConnectionState) {
        this.dbConnectionState = dbConnectionState;
    }

    public final DbConnectionState dbConnectionState() {
        return dbConnectionState;
    }

    @Override
    public OperationResult call() throws Exception {
        spinner.waitForScheduledStartTime(operation);

        DurationMeasurement durationMeasurement = DurationMeasurement.startMeasurementNow();

        OperationResult operationResult = executeOperation(operation);

        operationResult.setRunDuration(durationMeasurement.durationUntilNow());
        operationResult.setActualStartTime(durationMeasurement.startTime());
        operationResult.setOperationType(operation.type());
        operationResult.setScheduledStartTime(operation.scheduledStartTime());

        concurrentCompletionTimeService.submitCompletedTime(operation.scheduledStartTime());

        return operationResult;
    }

    protected abstract OperationResult executeOperation(A operation) throws DbException;

    @Override
    public String toString() {
        return String.format("OperationHandler [type=%s, operation=%s]", getClass().getName(), operation);
    }
}
