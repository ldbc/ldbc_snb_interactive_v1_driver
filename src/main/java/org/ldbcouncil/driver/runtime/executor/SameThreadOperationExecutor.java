package org.ldbcouncil.driver.runtime.executor;

import org.ldbcouncil.driver.ChildOperationGenerator;
import org.ldbcouncil.driver.Db;
import org.ldbcouncil.driver.Operation;
import org.ldbcouncil.driver.OperationHandlerRunnableContext;
import org.ldbcouncil.driver.WorkloadStreams;
import org.ldbcouncil.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.driver.runtime.coordination.CompletionTimeReader;
import org.ldbcouncil.driver.runtime.coordination.CompletionTimeWriter;
import org.ldbcouncil.driver.runtime.metrics.MetricsService;
import org.ldbcouncil.driver.runtime.scheduling.Spinner;
import org.ldbcouncil.driver.temporal.TimeSource;

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
            CompletionTimeWriter completionTimeWriter,
            CompletionTimeReader completionTimeReader,
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
                completionTimeWriter,
                completionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService );
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
