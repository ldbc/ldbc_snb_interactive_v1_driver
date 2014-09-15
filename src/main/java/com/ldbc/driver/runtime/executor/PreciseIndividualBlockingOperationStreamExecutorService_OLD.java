package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class PreciseIndividualBlockingOperationStreamExecutorService_OLD {
    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);
    private final PreciseIndividualBlockingOperationStreamExecutorServiceThread_OLD preciseIndividualBlockingOperationStreamExecutorServiceThreadOLD;
    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
    private final ConcurrentErrorReporter errorReporter;
    private AtomicBoolean executing = new AtomicBoolean(false);
    private AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);

    public PreciseIndividualBlockingOperationStreamExecutorService_OLD(TimeSource timeSource,
                                                                       ConcurrentErrorReporter errorReporter,
                                                                       Iterator<Operation<?>> operations,
                                                                       Spinner spinner,
                                                                       Spinner slightlyEarlySpinner,
                                                                       OperationHandlerExecutor operationHandlerExecutor,
                                                                       Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                                                                       Db db,
                                                                       LocalCompletionTimeWriter localCompletionTimeWriter,
                                                                       GlobalCompletionTimeReader globalCompletionTimeReader,
                                                                       ConcurrentMetricsService metricsService) {
        this.errorReporter = errorReporter;
        if (operations.hasNext()) {
            this.preciseIndividualBlockingOperationStreamExecutorServiceThreadOLD = new PreciseIndividualBlockingOperationStreamExecutorServiceThread_OLD(
                    timeSource,
                    operationHandlerExecutor,
                    errorReporter,
                    operations,
                    hasFinished,
                    spinner,
                    slightlyEarlySpinner,
                    forceThreadToTerminate,
                    operationClassifications,
                    db,
                    localCompletionTimeWriter,
                    globalCompletionTimeReader,
                    metricsService);
        } else {
            this.preciseIndividualBlockingOperationStreamExecutorServiceThreadOLD = null;
            executing.set(true);
            hasFinished.set(true);
            shutdown.set(false);
        }
    }

    synchronized public AtomicBoolean execute() {
        if (executing.get())
            return hasFinished;
        executing.set(true);
        preciseIndividualBlockingOperationStreamExecutorServiceThreadOLD.start();
        return hasFinished;
    }

    synchronized public void shutdown() throws OperationHandlerExecutorException {
        if (shutdown.get())
            throw new OperationHandlerExecutorException("Executor has already been shutdown");
        if (null != preciseIndividualBlockingOperationStreamExecutorServiceThreadOLD)
            doShutdown();
        shutdown.set(true);
    }

    private void doShutdown() {
        try {
            forceThreadToTerminate.set(true);
            preciseIndividualBlockingOperationStreamExecutorServiceThreadOLD.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
        } catch (Exception e) {
            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
            errorReporter.reportError(this, errMsg);
        }
    }
}