package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PreciseIndividualAsyncOperationStreamExecutorService {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TEMPORAL_UTIL.convert(10, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    private final TimeSource timeSource;
    private final PreciseIndividualAsyncOperationStreamExecutorServiceThread preciseIndividualAsyncOperationStreamExecutorServiceThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean executing = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);

    public PreciseIndividualAsyncOperationStreamExecutorService(TimeSource timeSource,
                                                                ConcurrentErrorReporter errorReporter,
                                                                WorkloadStreamDefinition streamDefinition,
                                                                Spinner spinner,
                                                                OperationHandlerExecutor operationHandlerExecutor,
                                                                Db db,
                                                                LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                ConcurrentMetricsService metricsService) {
        this.timeSource = timeSource;
        this.errorReporter = errorReporter;
        if (streamDefinition.dependencyOperations().hasNext() || streamDefinition.nonDependencyOperations().hasNext()) {
            this.preciseIndividualAsyncOperationStreamExecutorServiceThread = new PreciseIndividualAsyncOperationStreamExecutorServiceThread(
                    this.timeSource,
                    operationHandlerExecutor,
                    errorReporter,
                    streamDefinition,
                    hasFinished,
                    spinner,
                    forceThreadToTerminate,
                    db,
                    localCompletionTimeWriter,
                    globalCompletionTimeReader,
                    metricsService);
        } else {
            this.preciseIndividualAsyncOperationStreamExecutorServiceThread = null;
            executing.set(true);
            hasFinished.set(true);
            shutdown.set(false);
        }
    }

    synchronized public AtomicBoolean execute() {
        if (executing.get())
            return hasFinished;
        executing.set(true);
        preciseIndividualAsyncOperationStreamExecutorServiceThread.start();
        return hasFinished;
    }

    synchronized public void shutdown() throws OperationHandlerExecutorException {
        if (shutdown.get()) {
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        }
        if (null != preciseIndividualAsyncOperationStreamExecutorServiceThread)
            doShutdown();
        shutdown.set(true);
    }

    private void doShutdown() {
        try {
            forceThreadToTerminate.set(true);
            preciseIndividualAsyncOperationStreamExecutorServiceThread.join(SHUTDOWN_WAIT_TIMEOUT_AS_MILLI);
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }
}
