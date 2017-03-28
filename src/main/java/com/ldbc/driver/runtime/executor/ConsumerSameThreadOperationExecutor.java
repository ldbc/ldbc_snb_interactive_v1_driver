package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeService;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ConsumerSameThreadOperationExecutor implements OperationExecutor
{
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final Db db;
    private final AtomicLong uncompletedHandlers = new AtomicLong( 0 );
    private final Spinner spinner;
    private final TimeSource timeSource;
    private final ConcurrentErrorReporter errorReporter;
    private final MetricsService metricsService;

    public ConsumerSameThreadOperationExecutor( Db db, LocalCompletionTimeWriter localCompletionTimeWriterForAsynchronous, CompletionTimeService completionTimeService, Spinner spinner, TimeSource timeSource, ConcurrentErrorReporter errorReporter, MetricsService metricsService )
    {
        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriterForAsynchronous;
        this.spinner = spinner;
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
    }

    @Override
    public final void execute( Operation operation ) throws OperationExecutorException
    {
        uncompletedHandlers.incrementAndGet();
        OperationHandlerRunnableContext operationHandlerRunnableContext = null;
        try
        {
            operationHandlerRunnableContext = getInitializedHandlerFor( operation );
            operationHandlerRunnableContext.run();
        }
        catch ( Exception e )
        {
            throw new OperationExecutorException(
                    format( "Error while retrieving handler for operation\nOperation: %s", operation ), e );
        }
        finally
        {
            uncompletedHandlers.decrementAndGet();
            operationHandlerRunnableContext.cleanup();
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
    }

    @Override
    public long uncompletedOperationHandlerCount()
    {
        return uncompletedHandlers.get();
    }
}
