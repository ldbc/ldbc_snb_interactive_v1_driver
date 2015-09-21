package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class SameThreadOperationExecutor implements OperationExecutor
{
    private final AtomicLong uncompletedHandlers = new AtomicLong( 0 );
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever;
    private final ChildOperationGenerator childOperationGenerator;
    private final ChildOperationExecutor childOperationExecutor;

    public SameThreadOperationExecutor( Db db,
            WorkloadStreams.WorkloadStreamDefinition streamDefinition,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            GlobalCompletionTimeReader globalCompletionTimeReader,
            Spinner spinner,
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter,
            MetricsService metricsService,
            ChildOperationGenerator childOperationGenerator )
    {
        this.childOperationExecutor = new ChildOperationExecutor();
        this.childOperationGenerator = childOperationGenerator;
        this.operationHandlerRunnableContextRetriever = new OperationHandlerRunnableContextRetriever(
                streamDefinition,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService
        );
    }

    @Override
    public final void execute( Operation operation ) throws OperationExecutorException
    {
        uncompletedHandlers.incrementAndGet();
        OperationHandlerRunnableContext operationHandlerRunnableContext = null;
        try
        {
            operationHandlerRunnableContext =
                    operationHandlerRunnableContextRetriever.getInitializedHandlerFor( operation );
            operationHandlerRunnableContext.run();
            childOperationExecutor.execute(
                    childOperationGenerator,
                    operationHandlerRunnableContext.operation(),
                    operationHandlerRunnableContext.resultReporter().result(),
                    operationHandlerRunnableContext.resultReporter().actualStartTimeAsMilli(),
                    operationHandlerRunnableContext.resultReporter().runDurationAsNano(),
                    operationHandlerRunnableContextRetriever
            );
        }
        catch ( Throwable e )
        {
            throw new OperationExecutorException(
                    format( "Error retrieving or executing handler\n" +
                            "Operation: %s\n" +
                            "Handler Context:%s",
                            operation,
                            operationHandlerRunnableContext ),
                    e
            );
        }
        finally
        {
            uncompletedHandlers.decrementAndGet();
            operationHandlerRunnableContext.cleanup();
        }
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
