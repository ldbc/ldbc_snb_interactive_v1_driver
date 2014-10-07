package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.GctDependencyCheck;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

class PreciseIndividualAsyncOperationStreamExecutorServiceThread extends Thread {
    private static final Duration POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH = Duration.fromMilli(100);
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final TimeSource timeSource;
    private final OperationHandlerExecutor operationHandlerExecutor;
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean hasFinished;
    private final AtomicBoolean forcedTerminate;
    private final HandlerRetriever handlerRetriever;
    private final Duration durationToWaitForAllHandlersToFinishBeforeShutdown;

    public PreciseIndividualAsyncOperationStreamExecutorServiceThread(TimeSource timeSource,
                                                                      OperationHandlerExecutor operationHandlerExecutor,
                                                                      ConcurrentErrorReporter errorReporter,
                                                                      WorkloadStreamDefinition streamDefinition,
                                                                      AtomicBoolean hasFinished,
                                                                      Spinner spinner,
                                                                      AtomicBoolean forcedTerminate,
                                                                      Db db,
                                                                      LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                      GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                      ConcurrentMetricsService metricsService,
                                                                      Duration durationToWaitForAllHandlersToFinishBeforeShutdown) {
        super(PreciseIndividualAsyncOperationStreamExecutorServiceThread.class.getSimpleName() + "-" + System.currentTimeMillis());
        this.timeSource = timeSource;
        this.operationHandlerExecutor = operationHandlerExecutor;
        this.errorReporter = errorReporter;
        this.hasFinished = hasFinished;
        this.forcedTerminate = forcedTerminate;
        this.durationToWaitForAllHandlersToFinishBeforeShutdown = durationToWaitForAllHandlersToFinishBeforeShutdown;
        this.handlerRetriever = new HandlerRetriever(
                streamDefinition,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService);
    }

    @Override
    public void run() {
        long startTimeOfLastOperationAsNano = 0;
        while (handlerRetriever.hasNextHandler() && false == forcedTerminate.get()) {
            OperationHandler<?> handler;
            try {
                handler = handlerRetriever.nextHandler();
            } catch (Exception e) {
                String errMsg = String.format("Error while retrieving next handler\n%s",
                        ConcurrentErrorReporter.stackTraceToString(e));
                errorReporter.reportError(this, errMsg);
                continue;
            }
            startTimeOfLastOperationAsNano = handler.operation().scheduledStartTime().asNano();

            try {
                // --- BLOCKING CALL (when bounded queue is full) ---
                operationHandlerExecutor.execute(handler);
            } catch (OperationHandlerExecutorException e) {
                String errMsg = String.format("Error encountered while submitting operation for execution\n%s\n%s",
                        handler.operation(),
                        ConcurrentErrorReporter.stackTraceToString(e));
                errorReporter.reportError(this, errMsg);
                continue;
            }
        }

        boolean handlersFinishedInTime = awaitAllRunningHandlers(Time.fromNano(startTimeOfLastOperationAsNano), durationToWaitForAllHandlersToFinishBeforeShutdown);
        if (false == handlersFinishedInTime) {
            errorReporter.reportError(
                    this,
                    String.format(
                            "%s operation handlers did not complete in time (within %s of the time the last operation was submitted for execution)",
                            operationHandlerExecutor.uncompletedOperationHandlerCount(),
                            durationToWaitForAllHandlersToFinishBeforeShutdown
                    )
            );
        }
        this.hasFinished.set(true);
    }

    private boolean awaitAllRunningHandlers(Time startTimeOfLastOperation, Duration timeoutDuration) {
        long pollInterval = POLL_INTERVAL_WHILE_WAITING_FOR_LAST_HANDLER_TO_FINISH.asMilli();
        long timeoutTimeMs = startTimeOfLastOperation.plus(timeoutDuration).asMilli();
        while (timeSource.nowAsMilli() < timeoutTimeMs) {
            if (0 == operationHandlerExecutor.uncompletedOperationHandlerCount()) return true;
            if (forcedTerminate.get()) return true;
            Spinner.powerNap(pollInterval);
        }
        return false;
    }

    private static class HandlerRetriever {
        private final WorkloadStreamDefinition streamDefinition;
        private final Iterator<Operation<?>> gctNonWriteOperations;
        private final Iterator<Operation<?>> gctWriteOperations;
        private final Db db;
        private final LocalCompletionTimeWriter localCompletionTimeWriter;
        private final GlobalCompletionTimeReader globalCompletionTimeReader;
        private final Spinner spinner;
        private final TimeSource timeSource;
        private final ConcurrentErrorReporter errorReporter;
        private final ConcurrentMetricsService metricsService;
        OperationHandler<?> nextGctReadHandler;
        OperationHandler<?> nextGctWriteHandler;

        private HandlerRetriever(WorkloadStreamDefinition streamDefinition,
                                 Db db,
                                 LocalCompletionTimeWriter localCompletionTimeWriter,
                                 GlobalCompletionTimeReader globalCompletionTimeReader,
                                 Spinner spinner,
                                 TimeSource timeSource,
                                 ConcurrentErrorReporter errorReporter,
                                 ConcurrentMetricsService metricsService) {
            this.streamDefinition = streamDefinition;
            this.gctNonWriteOperations = streamDefinition.nonDependencyOperations();
            this.gctWriteOperations = streamDefinition.dependencyOperations();
            this.db = db;
            this.localCompletionTimeWriter = localCompletionTimeWriter;
            this.globalCompletionTimeReader = globalCompletionTimeReader;
            this.spinner = spinner;
            this.timeSource = timeSource;
            this.errorReporter = errorReporter;
            this.metricsService = metricsService;
            this.nextGctReadHandler = null;
            this.nextGctWriteHandler = null;
        }

        public boolean hasNextHandler() {
            return gctNonWriteOperations.hasNext() || gctWriteOperations.hasNext();
        }

        public OperationHandler<?> nextHandler() throws OperationHandlerExecutorException, CompletionTimeException {
            if (gctWriteOperations.hasNext() && null == nextGctWriteHandler) {
                Operation<?> nextGctWriteOperation = gctWriteOperations.next();
                nextGctWriteHandler = getAndInitializeHandler(nextGctWriteOperation, localCompletionTimeWriter);
                // submit initiated time as soon as possible so GCT/dependencies can advance as soon as possible
                nextGctWriteHandler.localCompletionTimeWriter().submitLocalInitiatedTime(nextGctWriteHandler.operation().scheduledStartTime());
                if (false == gctWriteOperations.hasNext()) {
                    // after last write operation, submit highest possible initiated time to ensure that GCT progresses to time of highest LCT write
                    nextGctWriteHandler.localCompletionTimeWriter().submitLocalInitiatedTime(Time.fromNano(Long.MAX_VALUE));
                }
            }
            if (gctNonWriteOperations.hasNext() && null == nextGctReadHandler) {
                Operation<?> nextGctReadOperation = gctNonWriteOperations.next();
                nextGctReadHandler = getAndInitializeHandler(nextGctReadOperation, DUMMY_LOCAL_COMPLETION_TIME_WRITER);
                // no need to submit initiated time for an operation that should not write to GCT
            }
            if (null != nextGctWriteHandler && null != nextGctReadHandler) {
                long nextGctWriteHandlerStartTime = nextGctWriteHandler.operation().scheduledStartTime().asNano();
                long nextGctReadHandlerStartTime = nextGctReadHandler.operation().scheduledStartTime().asNano();
                OperationHandler<?> nextHandler;
                if (nextGctReadHandlerStartTime < nextGctWriteHandlerStartTime) {
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

            if (streamDefinition.isDependentOperation(operation))
                operationHandler.addBeforeExecuteCheck(new GctDependencyCheck(globalCompletionTimeReader, operation, errorReporter));

            return operationHandler;
        }
    }
}
