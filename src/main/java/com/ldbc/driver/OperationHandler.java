package com.ldbc.driver;

import java.util.concurrent.Callable;

import com.ldbc.driver.temporal.DurationMeasurement;

public abstract class OperationHandler<A extends Operation<?>> implements Callable<OperationResult>
{
    private A operation;
    private DbConnectionState dbConnectionState;

    public final void setOperation( Operation<?> operation )
    {
        this.operation = (A) operation;
    }

    public final void setDbConnectionState( DbConnectionState dbConnectionState )
    {
        this.dbConnectionState = dbConnectionState;
    }

    public final DbConnectionState dbConnectionState()
    {
        return dbConnectionState;
    }

    @Override
    public OperationResult call() throws Exception
    {
        DurationMeasurement durationMeasurement = DurationMeasurement.startMeasurementNow();

        OperationResult operationResult = executeOperation( operation );

        operationResult.setRunDuration( durationMeasurement.durationUntilNow() );
        operationResult.setActualStartTime( durationMeasurement.startTime() );
        operationResult.setOperationType( operation.type() );
        operationResult.setScheduledStartTime( operation.scheduledStartTime() );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation ) throws DbException;

    @Override
    public String toString()
    {
        return String.format( "OperationHandler [type=%s, operation=%s]", getClass().getName(), operation );
    }
}
