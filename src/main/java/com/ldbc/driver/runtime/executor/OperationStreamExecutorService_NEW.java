package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.temporal.TemporalUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class OperationStreamExecutorService_NEW {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    public static final long SHUTDOWN_WAIT_TIMEOUT_AS_MILLI = TEMPORAL_UTIL.convert(10, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);

    private final OperationStreamExecutorServiceThread_NEW operationStreamExecutorServiceThread;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private final AtomicBoolean executing = new AtomicBoolean(false);
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);

    public OperationStreamExecutorService_NEW(ConcurrentErrorReporter errorReporter,
                                              WorkloadStreamDefinition streamDefinition,
                                              OperationExecutor_NEW operationExecutor,
                                              LocalCompletionTimeWriter localCompletionTimeWriter) {
        this.errorReporter = errorReporter;
        if (streamDefinition.dependencyOperations().hasNext() || streamDefinition.nonDependencyOperations().hasNext()) {
            this.operationStreamExecutorServiceThread = new OperationStreamExecutorServiceThread_NEW(
                    operationExecutor,
                    errorReporter,
                    streamDefinition,
                    hasFinished,
                    forceThreadToTerminate,
                    localCompletionTimeWriter);
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

    synchronized public void shutdown(long shutdownWait) throws OperationExecutorException {
        if (shutdown.get())
            throw new OperationExecutorException("Executor has already been shutdown");
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