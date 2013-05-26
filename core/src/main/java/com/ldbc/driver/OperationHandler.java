package com.ldbc.driver;

import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

public abstract class OperationHandler<A extends Operation<?>> implements Callable<OperationResult>
{
    private static Logger logger = Logger.getLogger( OperationHandler.class );

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
        operationResult.setScheduledStartTime( operation.getScheduledStartTime() );
        operationResult.setActualStartTime( actualStartTime );
        operationResult.setRunTime( actualEndTime - actualStartTime );

        return operationResult;
    }

    protected abstract OperationResult executeOperation( A operation );

    @Override
    public String toString()
    {
        return String.format( "OperationHandler [type=%s, operation=%s]", getClass().getName(), operation );

    }
}
