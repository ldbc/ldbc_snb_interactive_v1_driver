package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ConsumerThreadPoolOperationExecutor implements OperationExecutor
{
    private final ExecutorService threadPoolExecutorService;
    private final AtomicBoolean shutdown = new AtomicBoolean( false );
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final Db db;
    private final AtomicLong uncompletedHandlers = new AtomicLong( 0 );
    private final Spinner spinner;
    private final TimeSource timeSource;
    private final ConcurrentErrorReporter errorReporter;
    private final MetricsService metricsService;

    public ConsumerThreadPoolOperationExecutor(
            int threadCount,
            int boundedQueueSize,
            Db db,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            GlobalCompletionTimeReader globalCompletionTimeReader,
            Spinner spinner,
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter,
            MetricsService metricsService )
    {
        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.spinner = spinner;
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
        ThreadFactory threadFactory = new ThreadFactory()
        {
            private final long factoryTimeStampId = System.currentTimeMillis();
            int count = 0;

            @Override
            public Thread newThread( Runnable runnable )
            {
                return new Thread(
                        runnable,
                        ConsumerThreadPoolOperationExecutor.class.getSimpleName() + "-id(" + factoryTimeStampId + ")" +
                                "-thread(" + count++ + ")"
                );
            }
        };
        this.threadPoolExecutorService = Executors.newFixedThreadPool( threadCount, threadFactory );
    }

    @Override
    public final void execute( Operation operation ) throws OperationExecutorException
    {
        uncompletedHandlers.incrementAndGet();
        try
        {
            OperationHandlerRunnableContext operationHandlerRunnableContext = getInitializedHandlerFor( operation );
            threadPoolExecutorService.execute( operationHandlerRunnableContext );
        }
        catch ( Throwable e )
        {
            throw new OperationExecutorException(
                    format( "Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString( e ) ),
                    e
            );
        }
    }

    private OperationHandlerRunnableContext getInitializedHandlerFor( Operation operation ) throws OperationExecutorException, OperationException
    {
        OperationHandlerRunnableContext operationHandlerRunnableContext = null;
        try
        {
            operationHandlerRunnableContext = db.getOperationHandlerRunnableContext( operation );
        }
        catch ( Exception e )
        {
            throw new OperationExecutorException(
                    format( "Error while retrieving handler for operation\nOperation: %s", operation ), e );
        }
        operationHandlerRunnableContext.init( timeSource, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService );
        return operationHandlerRunnableContext;
    }

    @Override
    synchronized public final void shutdown( long waitAsMilli ) throws OperationExecutorException
    {
        if ( shutdown.get() )
        {
            throw new OperationExecutorException( "Executor has already been shutdown" );
        }
        try
        {
            threadPoolExecutorService.shutdown();
            boolean allHandlersCompleted =
                    threadPoolExecutorService.awaitTermination( waitAsMilli, TimeUnit.MILLISECONDS );
            if ( false == allHandlersCompleted )
            {
                List<Runnable> stillRunningThreads = threadPoolExecutorService.shutdownNow();
                if ( false == stillRunningThreads.isEmpty() )
                {
                    String errMsg = format(
                            "%s shutdown before all handlers could complete\n%s handlers were queued for execution " +
                                    "but not yet started\n%s handlers were mid-execution",
                            getClass().getSimpleName(),
                            stillRunningThreads.size(),
                            uncompletedHandlers.get() - stillRunningThreads.size() );
                    throw new OperationExecutorException( errMsg );
                }
            }
        }
        catch ( Throwable e )
        {
            throw new OperationExecutorException( "Error encountered while trying to shutdown", e );
        }
        finally
        {
            shutdown.set( true );
        }
    }

    @Override
    public long uncompletedOperationHandlerCount()
    {
        return uncompletedHandlers.get();
    }

}
