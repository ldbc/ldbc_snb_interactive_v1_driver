package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.GctDependencyCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.util.Iterator;
import java.util.Set;

// TODO test
class DependencyAndNonDependencyHandlersRetriever {
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();
    private final Iterator<Operation<?>> gctNonWriteOperations;
    private final Iterator<Operation<?>> gctWriteOperations;
    private final Db db;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    private final GlobalCompletionTimeReader globalCompletionTimeReader;
    private final Spinner spinner;
    private final TimeSource timeSource;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentMetricsService metricsService;
    private final Set<Class<? extends Operation<?>>> dependentOperationTypes;
    OperationHandler<?> nextGctReadHandler;
    OperationHandler<?> nextGctWriteHandler;

    DependencyAndNonDependencyHandlersRetriever(WorkloadStreams.WorkloadStreamDefinition streamDefinition,
                                                Db db,
                                                LocalCompletionTimeWriter localCompletionTimeWriter,
                                                GlobalCompletionTimeReader globalCompletionTimeReader,
                                                Spinner spinner,
                                                TimeSource timeSource,
                                                ConcurrentErrorReporter errorReporter,
                                                ConcurrentMetricsService metricsService) {
        this.gctNonWriteOperations = streamDefinition.nonDependencyOperations();
        this.gctWriteOperations = streamDefinition.dependencyOperations();
        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
        this.globalCompletionTimeReader = globalCompletionTimeReader;
        this.spinner = spinner;
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        this.metricsService = metricsService;
        this.dependentOperationTypes = streamDefinition.dependentOperationTypes();
        this.nextGctReadHandler = null;
        this.nextGctWriteHandler = null;
    }

    public boolean hasNextHandler() {
        return gctNonWriteOperations.hasNext() || gctWriteOperations.hasNext();
    }

    /*
    1. get next handlers (both gct writing & non gct writing)
    2. initialize handlers
    3. submit initiated time, for gct writing handler
    4. return handler with lowest scheduled start time
     */
    public OperationHandler<?> nextHandler() throws OperationHandlerExecutorException, CompletionTimeException {
        // get and initialize next gct writing handler
        if (gctWriteOperations.hasNext() && null == nextGctWriteHandler) {
            Operation<?> nextGctWriteOperation = gctWriteOperations.next();
            nextGctWriteHandler = getAndInitializeHandler(nextGctWriteOperation, localCompletionTimeWriter);
            // TODO remove later, but at the moment Add Person and Add Friendship are in the same stream, and only Add Person should introduce a dependency
            if (false == nextGctWriteOperation.getClass().equals(LdbcUpdate8AddFriendship.class))
                // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
                nextGctWriteHandler.localCompletionTimeWriter().submitLocalInitiatedTime(nextGctWriteHandler.operation().scheduledStartTimeAsMilli());
            if (false == gctWriteOperations.hasNext()) {
                // after last write operation, submit highest possible initiated time to ensure that GCT progresses to time of highest LCT write
                nextGctWriteHandler.localCompletionTimeWriter().submitLocalInitiatedTime(Long.MAX_VALUE);
            }
        }
        // get and initialize next non gct writing handler
        if (gctNonWriteOperations.hasNext() && null == nextGctReadHandler) {
            Operation<?> nextGctReadOperation = gctNonWriteOperations.next();
            nextGctReadHandler = getAndInitializeHandler(nextGctReadOperation, DUMMY_LOCAL_COMPLETION_TIME_WRITER);
            // no need to submit initiated time for an operation that should not write to GCT
        }
        // return handler with lowest start time
        if (null != nextGctWriteHandler && null != nextGctReadHandler) {
            long nextGctWriteHandlerStartTimeAsMilli = nextGctWriteHandler.operation().scheduledStartTimeAsMilli();
            long nextGctReadHandlerStartTimeAsMilli = nextGctReadHandler.operation().scheduledStartTimeAsMilli();
            OperationHandler<?> nextHandler;
            if (nextGctReadHandlerStartTimeAsMilli < nextGctWriteHandlerStartTimeAsMilli) {
                nextHandler = nextGctReadHandler;
                nextGctReadHandler = null;
            } else {
                nextHandler = nextGctWriteHandler;
                nextGctWriteHandler = null;
            }
            return nextHandler;
        } else if (null == nextGctWriteHandler && null != nextGctReadHandler) {
            OperationHandler<?> nextHandler = nextGctReadHandler;
            nextGctReadHandler = null;
            return nextHandler;
        } else if (null != nextGctWriteHandler && null == nextGctReadHandler) {
            OperationHandler<?> nextHandler = nextGctWriteHandler;
            nextGctWriteHandler = null;
            return nextHandler;
        } else {
            throw new OperationHandlerExecutorException("Unexpected error in " + getClass().getSimpleName());
        }
    }

    private OperationHandler<?> getAndInitializeHandler(Operation<?> operation, LocalCompletionTimeWriter localCompletionTimeWriterForHandler) throws OperationHandlerExecutorException {
        OperationHandler<?> operationHandler;
        try {
            operationHandler = db.getOperationHandler(operation);
        } catch (DbException e) {
            throw new OperationHandlerExecutorException(String.format("Error while retrieving handler for operation\nOperation: %s", operation));
        }

        try {
            operationHandler.init(timeSource, spinner, operation, localCompletionTimeWriterForHandler, errorReporter, metricsService);
        } catch (OperationException e) {
            throw new OperationHandlerExecutorException(String.format("Error while initializing handler for operation\nOperation: %s", operation));
        }

        if (dependentOperationTypes.contains(operation.getClass()))
            operationHandler.addBeforeExecuteCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));

        return operationHandler;
    }
}
