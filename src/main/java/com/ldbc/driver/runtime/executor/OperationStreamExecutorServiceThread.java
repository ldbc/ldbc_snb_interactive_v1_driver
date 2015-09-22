package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.scheduling.Spinner;

import java.util.concurrent.atomic.AtomicBoolean;

class OperationStreamExecutorServiceThread extends Thread
{
    private static final long POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI = 100;

    private final OperationExecutor operationExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final InitiatedTimeSubmittingOperationRetriever initiatedTimeSubmittingOperationRetriever;

    public OperationStreamExecutorServiceThread( OperationExecutor operationExecutor,
            ConcurrentErrorReporter errorReporter,
            WorkloadStreamDefinition streamDefinition,
            AtomicBoolean hasFinished,
            AtomicBoolean forcedTerminate,
            LocalCompletionTimeWriter localCompletionTimeWriter )
    {
        super( OperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
        this.operationExecutor = operationExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.initiatedTimeSubmittingOperationRetriever = new InitiatedTimeSubmittingOperationRetriever(
                streamDefinition,
                localCompletionTimeWriter
        );
    }

    @Override
    public void run()
    {
        try
        {
            while ( initiatedTimeSubmittingOperationRetriever.hasNextOperation() && !forcedTerminate.get() )
            {
                Operation operation = initiatedTimeSubmittingOperationRetriever.nextOperation();
                // --- BLOCKING CALL (when bounded queue is full) ---
                operationExecutor.execute( operation );
            }
        }
        catch ( Throwable e )
        {
            errorReporter.reportError( this, ConcurrentErrorReporter.stackTraceToString( e ) );
        }
        finally
        {
            while ( 0 < operationExecutor.uncompletedOperationHandlerCount() && !forcedTerminate.get() )
            {
                Spinner.powerNap( POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH_AS_MILLI );
            }
            this.hasFinished.set( true );
        }
    }
}