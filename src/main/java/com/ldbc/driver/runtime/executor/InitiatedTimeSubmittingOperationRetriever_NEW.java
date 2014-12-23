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

import java.util.Iterator;

// TODO test
class InitiatedTimeSubmittingOperationRetriever_NEW {
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();
    private final Iterator<Operation<?>> nonDependencyOperations;
    private final Iterator<Operation<?>> dependencyOperations;
    //    private final Db db;
    private final LocalCompletionTimeWriter localCompletionTimeWriter;
    //    private final GlobalCompletionTimeReader globalCompletionTimeReader;
//    private final Spinner spinner;
//    private final TimeSource timeSource;
    private final ConcurrentErrorReporter errorReporter;
    //    private final ConcurrentMetricsService metricsService;
//    private final Set<Class<? extends Operation<?>>> dependentOperationTypes;
//    OperationHandlerRunnableContext nextGctReadHandlerRunner;
//    OperationHandlerRunnableContext nextGctWriteHandlerRunner;
    private Operation nextNonDependencyOperation = null;
    private Operation nextDependencyOperation = null;

    InitiatedTimeSubmittingOperationRetriever_NEW(WorkloadStreams.WorkloadStreamDefinition streamDefinition,
                                                  Db db,
                                                  LocalCompletionTimeWriter localCompletionTimeWriter,
                                                  GlobalCompletionTimeReader globalCompletionTimeReader,
                                                  Spinner spinner,
                                                  TimeSource timeSource,
                                                  ConcurrentErrorReporter errorReporter,
                                                  ConcurrentMetricsService metricsService) {
        this.nonDependencyOperations = streamDefinition.nonDependencyOperations();
        this.dependencyOperations = streamDefinition.dependencyOperations();
//        this.db = db;
        this.localCompletionTimeWriter = localCompletionTimeWriter;
//        this.globalCompletionTimeReader = globalCompletionTimeReader;
//        this.spinner = spinner;
//        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
//        this.metricsService = metricsService;
//        this.dependentOperationTypes = streamDefinition.dependentOperationTypes();
//        this.nextGctReadHandlerRunner = null;
//        this.nextGctWriteHandlerRunner = null;
    }

    public boolean hasNextHandlerRunner() {
        return nonDependencyOperations.hasNext() || dependencyOperations.hasNext();
    }

    /*
    1. get next handlers (both gct writing & non gct writing)
    2. initialize handlers
    3. submit initiated time, for gct writing handler
    4. return handler with lowest scheduled start time
     */
    public OperationHandlerRunnableContext nextHandlerRunner() throws OperationHandlerExecutorException, CompletionTimeException {
        // get and initialize next gct writing handler
        if (dependencyOperations.hasNext() && null == nextDependencyOperation) {
            nextDependencyOperation = dependencyOperations.next();
            // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
            localCompletionTimeWriter.submitLocalInitiatedTime(nextDependencyOperation.timeStamp());
            if (false == dependencyOperations.hasNext()) {
                // after last write operation, submit highest possible initiated time to ensure that GCT progresses to time of highest LCT write
                localCompletionTimeWriter.submitLocalInitiatedTime(Long.MAX_VALUE);
            }
        }
        // get and initialize next non gct writing handler
        if (nonDependencyOperations.hasNext() && null == nextGctReadHandlerRunner) {
            Operation<?> nextGctReadOperation = nonDependencyOperations.next();
            nextGctReadHandlerRunner = getAndInitializeHandler(nextGctReadOperation, DUMMY_LOCAL_COMPLETION_TIME_WRITER);
            // no need to submit initiated time for an operation that should not write to GCT
        }
        // return handler with lowest start time
        if (null != nextGctWriteHandlerRunner && null != nextGctReadHandlerRunner) {
            long nextGctWriteHandlerStartTimeAsMilli = nextGctWriteHandlerRunner.operation().timeStamp();
            long nextGctReadHandlerStartTimeAsMilli = nextGctReadHandlerRunner.operation().timeStamp();
            OperationHandlerRunnableContext nextHandlerRunner;
            if (nextGctReadHandlerStartTimeAsMilli < nextGctWriteHandlerStartTimeAsMilli) {
                nextHandlerRunner = nextGctReadHandlerRunner;
                nextGctReadHandlerRunner = null;
            } else {
                nextHandlerRunner = nextGctWriteHandlerRunner;
                nextGctWriteHandlerRunner = null;
            }
            return nextHandlerRunner;
        } else if (null == nextGctWriteHandlerRunner && null != nextGctReadHandlerRunner) {
            OperationHandlerRunnableContext nextHandlerRunner = nextGctReadHandlerRunner;
            nextGctReadHandlerRunner = null;
            return nextHandlerRunner;
        } else if (null != nextGctWriteHandlerRunner && null == nextGctReadHandlerRunner) {
            OperationHandlerRunnableContext nextHandlerRunner = nextGctWriteHandlerRunner;
            nextGctWriteHandlerRunner = null;
            return nextHandlerRunner;
        } else {
            throw new OperationHandlerExecutorException("Unexpected error in " + getClass().getSimpleName());
        }
    }

    private OperationHandlerRunnableContext getAndInitializeHandler(Operation<?> operation, LocalCompletionTimeWriter localCompletionTimeWriterForHandler) throws OperationHandlerExecutorException {
        OperationHandlerRunnableContext operationHandlerRunnableContext;
        try {
            operationHandlerRunnableContext = db.getOperationHandlerRunnableContext(operation);
        } catch (DbException e) {
            throw new OperationHandlerExecutorException(String.format("Error while retrieving handler for operation\nOperation: %s", operation));
        }

        try {
            operationHandlerRunnableContext.init(timeSource, spinner, operation, localCompletionTimeWriterForHandler, errorReporter, metricsService);
        } catch (OperationException e) {
            throw new OperationHandlerExecutorException(String.format("Error while initializing handler for operation\nOperation: %s", operation));
        }

        if (dependentOperationTypes.contains(operation.getClass()))
            operationHandlerRunnableContext.setBeforeExecuteCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));

        return operationHandlerRunnableContext;
    }
}
