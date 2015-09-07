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
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer;
    private final ChildOperationGenerator childOperationGenerator;

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
        this.childOperationGenerator = childOperationGenerator;
        this.operationHandlerRunnableContextInitializer = new OperationHandlerRunnableContextRetriever(
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
        try
        {
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    operationHandlerRunnableContextInitializer.getInitializedHandlerFor( operation );
            operationHandlerRunnableContext.run();
            if ( null != childOperationGenerator )
            {
                double state = childOperationGenerator.initialState();
                operation = childOperationGenerator.nextOperation(
                        state,
                        operationHandlerRunnableContext.operation(),
                        operationHandlerRunnableContext.resultReporter().result(),
                        operationHandlerRunnableContext.resultReporter().actualStartTimeAsMilli(),
                        operationHandlerRunnableContext.resultReporter().runDurationAsNano()
                );
                while ( null != operation )
                {
                    OperationHandlerRunnableContext childOperationHandlerRunnableContext =
                            operationHandlerRunnableContextInitializer.getInitializedHandlerFor( operation );
                    childOperationHandlerRunnableContext.run();
                    state = childOperationGenerator.updateState( state, operation.type() );
                    operation = childOperationGenerator.nextOperation(
                            state,
                            childOperationHandlerRunnableContext.operation(),
                            childOperationHandlerRunnableContext.resultReporter().result(),
                            childOperationHandlerRunnableContext.resultReporter().actualStartTimeAsMilli(),
                            childOperationHandlerRunnableContext.resultReporter().runDurationAsNano()
                    );
                    childOperationHandlerRunnableContext.cleanup();
                }
            }
            operationHandlerRunnableContext.cleanup();
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
        finally
        {
            uncompletedHandlers.decrementAndGet();
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
