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

public class OperationStreamExecutorService {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    public static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TEMPORAL_UTIL.convert(10, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    private final OperationStreamExecutorServiceThread operationStreamExecutorServiceThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean executing = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);

    public OperationStreamExecutorService(TimeSource timeSource,
                                          ConcurrentErrorReporter errorReporter,
                                          WorkloadStreamDefinition streamDefinition,
                                          Spinner spinner,
                                          OperationHandlerExecutor operationHandlerExecutor,
                                          Db db,
                                          LocalCompletionTimeWriter localCompletionTimeWriter,
                                          GlobalCompletionTimeReader globalCompletionTimeReader,
                                          ConcurrentMetricsService metricsService) {
        this.errorReporter = errorReporter;
        if (streamDefinition.dependencyOperations().hasNext() || streamDefinition.nonDependencyOperations().hasNext()) {
            this.operationStreamExecutorServiceThread = new OperationStreamExecutorServiceThread(
                    timeSource,
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
            this.operationStreamExecutorServiceThread = null;
            executing.set(true);
            hasFinished.set(true);
            shutdown.set(false);
        }
    }

    synchronized public AtomicBoolean execute() {
        if (executing.get())
            return hasFinished;
        executing.set(true);
        operationStreamExecutorServiceThread.start();
        return hasFinished;
    }

    synchronized public void shutdown(long shutdownWait) throws OperationHandlerExecutorException {
        if (shutdown.get())
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        if (null != operationStreamExecutorServiceThread)
            doShutdown(shutdownWait);
        shutdown.set(true);
    }

    private void doShutdown(long shutdownWait) {
        try {
            forceThreadToTerminate.set(true);
            operationStreamExecutorServiceThread.join(shutdownWait);
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }
}