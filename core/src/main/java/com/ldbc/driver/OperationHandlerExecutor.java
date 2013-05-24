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

    private long handlersAlreadyExecuted = 0;
    private long handlersStillExecuting = 0;

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
        handlersStillExecuting++;
    }

    public final OperationResult nextOperationResult() throws InterruptedException, ExecutionException
    {
        Future<OperationResult> operationResultFuture = operationHandlerCompletionPool.poll( POLL_TIMEOUT_MS,
                TimeUnit.MILLISECONDS );
        if ( null == operationResultFuture ) return null;
        OperationResult operationResult = operationResultFuture.get();
        handlersAlreadyExecuted++;
        handlersStillExecuting--;
        return operationResult;
    }

    public final void shutdown()
    {
        threadPool.shutdown();
    }

    public final int getThreadCount()
    {
        return threadCount;
    }

    public final long getHandlersAlreadyExecuted()
    {
        return handlersAlreadyExecuted;
    }

    public final long getHandlersStillExecuting()
    {
        return handlersStillExecuting;
    }
}
