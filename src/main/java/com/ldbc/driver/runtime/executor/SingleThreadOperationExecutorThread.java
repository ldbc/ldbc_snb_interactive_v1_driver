package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.QueueEventFetcher;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class SingleThreadOperationExecutorThread extends Thread
{
    private final QueueEventFetcher<Operation> operationQueueEventFetcher;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicLong uncompletedHandlers;
    private final AtomicBoolean forcedShutdownRequested = new AtomicBoolean( false );
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever;
    private final ChildOperationGenerator childOperationGenerator;

    SingleThreadOperationExecutorThread( Queue<Operation> operationHandlerRunnerQueue,
            ConcurrentErrorReporter errorReporter,
            AtomicLong uncompletedHandlers,
            OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever,
            ChildOperationGenerator childOperationGenerator )
    {
        super( SingleThreadOperationExecutorThread.class.getSimpleName() + "-" + System.currentTimeMillis() );
        this.operationQueueEventFetcher = QueueEventFetcher.queueEventFetcherFor( operationHandlerRunnerQueue );
        this.errorReporter = errorReporter;
        this.uncompletedHandlers = uncompletedHandlers;
        this.operationHandlerRunnableContextRetriever = operationHandlerRunnableContextRetriever;
        this.childOperationGenerator = childOperationGenerator;
    }

    @Override
    public void run()
    {
        Operation operation = null;
        try
        {
            operation = operationQueueEventFetcher.fetchNextEvent();
            while ( operation != SingleThreadOperationExecutor.TERMINATE_OPERATION &&
                    false == forcedShutdownRequested.get() )
            {
                OperationHandlerRunnableContext operationHandlerRunnableContext =
                        operationHandlerRunnableContextRetriever.getInitializedHandlerFor( operation );
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
                                operationHandlerRunnableContextRetriever.getInitializedHandlerFor( operation );
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
                uncompletedHandlers.decrementAndGet();
                operation = operationQueueEventFetcher.fetchNextEvent();
            }
        }
        catch ( Exception e )
        {
            errorReporter.reportError(
                    this,
                    format( "Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString( e ) )
            );
        }
    }

    void forceShutdown()
    {
        forcedShutdownRequested.set( true );
    }
}
