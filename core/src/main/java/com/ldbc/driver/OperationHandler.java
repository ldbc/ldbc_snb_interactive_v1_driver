package com.ldbc.driver;

import java.util.concurrent.Callable;

public abstract class OperationHandler<A extends Operation<?>> implements Callable<OperationResult>
{
    private A operation;

    public final void setOperation( Operation<?> operation )
    {
        this.operation = (A) operation;
    }

    @Override
    public OperationResult call() throws Exception
    {
        long actualStartTime = System.nanoTime();
        OperationResult operationResult = executeOperation( operation );
        long actualEndTime = System.nanoTime();

        operationResult.setOperationType( operation.getClass().getName() );
        // TODO ScheduledStartTime
        operationResult.setScheduledStartTime( -1 );
        operationResult.setActualStartTime( actualStartTime );
        // TODO runtime: why /1000? why int?
        operationResult.setRunTime( ( actualEndTime - actualStartTime ) / 1000 );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation );

    @Override
    public String toString()
    {
        return String.format( "OperationHandler [type=%s, operation=%s]", getClass().getName(), operation );

    }
}
