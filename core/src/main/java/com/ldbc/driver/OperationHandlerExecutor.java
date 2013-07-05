package com.ldbc.driver;

import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

// TODO ThreadPoolOperationHandlerExecutor
// TODO NetworkOperationHandlerExecutor
public class OperationHandlerExecutor
{
    private static Logger logger = Logger.getLogger( OperationHandlerExecutor.class );

    private final long POLL_TIMEOUT_MS = 100;

    private final int threadCount;
    private final ExecutorService threadPool;
    private final CompletionService<OperationResult> operationHandlerCompletionPool;

    private long retrievedResults = 0;
    private long submittedHandlers = 0;

    private boolean shutdown = false;

    public static void main( String[] args )
    {
        final int THREAD_COUNT = 4;
        final OperationHandlerExecutor operationHandlerExecutor = new OperationHandlerExecutor( THREAD_COUNT );

        OperationHandler<?> operationHandler = null;
        operationHandlerExecutor.execute( operationHandler );
        operationHandlerExecutor.shutdown();
    }

    public OperationHandlerExecutor( int threadCount )
    {
        this.threadCount = threadCount;
        this.threadPool = createThreadPool( threadCount );
        this.operationHandlerCompletionPool = new ExecutorCompletionService<OperationResult>( threadPool );
    }

    private ExecutorService createThreadPool( int threadCount )
    {
        return Executors.newFixedThreadPool( threadCount );
    }

    public final void execute( OperationHandler<?> operationHandler )
    {
        operationHandlerCompletionPool.submit( operationHandler );
        submittedHandlers++;
    }

    /**
     * Get next OperationResult returned by a submitted OperationHandler. If
     * none are currently available, return null.
     * 
     * @return OperationResult if one available, null otherwise
     */
    public final OperationResult nextOperationResult() throws InterruptedException, ExecutionException
    {
        Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.poll( POLL_TIMEOUT_MS,
                TimeUnit.MILLISECONDS );
        if ( null == operationResultFuture ) return null;
        OperationResult operationResult = operationResultFuture.get();
        retrievedResults++;
        return operationResult;
    }

    /**
     * Get next OperationResult returned by a submitted OperationHandler. Blocks
     * to wait for the next OperationResult if any OperationHandler is still
     * running. Returns immediately with null otherwise.
     * 
     * @return OperationResult if any are pending, null otherwise
     */
    public final OperationResult waitForNextOperationResult() throws InterruptedException, ExecutionException
    {
        if ( submittedHandlers == retrievedResults ) return null;
        Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.take();
        OperationResult operationResult = operationResultFuture.get();
        retrievedResults++;
        return operationResult;
    }

    public final void shutdown()
    {
        if ( true == shutdown ) return;
        threadPool.shutdown();
        shutdown = true;
    }

    public final int getThreadCount()
    {
        return threadCount;
    }

    public final long getHandlersAlreadyExecuted()
    {
        return retrievedResults;
    }

    public final long getHandlersStillExecuting()
    {
        return submittedHandlers;
    }
}
