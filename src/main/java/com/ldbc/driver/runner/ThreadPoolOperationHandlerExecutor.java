package com.ldbc.driver.runner;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResult;
import com.ldbc.driver.util.temporal.Duration;

public class ThreadPoolOperationHandlerExecutor implements OperationHandlerExecutor
{
    private final Duration POLL_TIMEOUT = Duration.fromMilli( 100 );

    private final ExecutorService threadPool;
    private final CompletionService<OperationResult> operationHandlerCompletionPool;

    private long retrievedResults = 0;
    private long submittedHandlers = 0;

    private boolean shutdown = false;

    public ThreadPoolOperationHandlerExecutor( int threadCount )
    {
        this.threadPool = createThreadPool( threadCount );
        this.operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>( threadPool );
    }

    private ExecutorService createThreadPool( int threadCount )
    {
        return Executors.newFixedThreadPool( threadCount );
    }

    @Override
    public final void execute( OperationHandler<?> operationHandler )
    {
        operationHandlerCompletionPool.submit( operationHandler );
        submittedHandlers++;
    }

    @Override
    public final OperationResult nextOperationResultNonBlocking() throws OperationHandlerExecutorException
    {
        try
        {
            Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.poll(
                    POLL_TIMEOUT.asMilli(), TimeUnit.MILLISECONDS );
            if ( null == operationResultFuture ) return null;
            OperationResult operationResult;
            operationResult = operationResultFuture.get();
            retrievedResults++;
            return operationResult;
        }
        catch ( Exception e )
        {
            throw new OperationHandlerExecutorException( e.getCause() );
        }
    }

    @Override
    public final OperationResult nextOperationResultBlocking() throws OperationHandlerExecutorException
    {
        try
        {
            if ( submittedHandlers == retrievedResults ) return null;
            Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.take();
            OperationResult operationResult = operationResultFuture.get();
            retrievedResults++;
            return operationResult;
        }
        catch ( Exception e )
        {
            throw new OperationHandlerExecutorException( e.getCause() );
        }
    }

    @Override
    public final void shutdown()
    {
        if ( true == shutdown ) return;
        threadPool.shutdown();
        shutdown = true;
    }
}
