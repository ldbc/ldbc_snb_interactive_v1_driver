package com.ldbc.driver;

import java.util.concurrent.Callable;

import com.ldbc.driver.util.time.DurationMeasurement;
import com.ldbc.driver.util.time.Time;

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

    public final DbConnectionState getDbConnectionState()
    {
        return dbConnectionState;
    }

    @Override
    public OperationResult call() throws Exception
    {
        DurationMeasurement durationMeasurement = DurationMeasurement.startMeasurementNow();
        Time actualStartTime = Time.now();

        OperationResult operationResult = executeOperation( operation );

        operationResult.setOperationType( operation.getClass().getName() );
        operationResult.setScheduledStartTime( operation.getScheduledStartTime() );
        operationResult.setActualStartTime( actualStartTime );
        operationResult.setRunTime( durationMeasurement.getDurationUntilNow() );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation ) throws DbException;

    @Override
    public String toString()
    {
        return String.format( "OperationHandler [type=%s, operation=%s]", getClass().getName(), operation );

    }
}
