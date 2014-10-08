package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

@Deprecated
public class UniformWindowedOperationStreamExecutorService {
//    private static final Duration SHUTDOWN_WAIT_TIMEOUT = Duration.fromSeconds(5);
//
//    private final UniformWindowedOperationStreamExecutorServiceThread uniformWindowedOperationStreamExecutorServiceThread;
//    private final AtomicBoolean hasFinished = new AtomicBoolean(false);
//    private final ConcurrentErrorReporter errorReporter;
//    private final AtomicBoolean executing = new AtomicBoolean(false);
//    private final AtomicBoolean shutdown = new AtomicBoolean(false);
//    private final AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);
//
//    public UniformWindowedOperationStreamExecutorService(TimeSource timeSource,
//                                                         ConcurrentErrorReporter errorReporter,
//                                                         Iterator<Operation<?>> operations,
//                                                         OperationHandlerExecutor operationHandlerExecutor,
//                                                         Spinner spinner,
//                                                         Time firstWindowStartTime,
//                                                         Duration windowSize,
//                                                         Db db,
//                                                         Map<Class<? extends Operation>, OperationClassification> operationClassifications,
//                                                         LocalCompletionTimeWriter localCompletionTimeWriter,
//                                                         GlobalCompletionTimeReader globalCompletionTimeReader,
//                                                         ConcurrentMetricsService metricsService,
//                                                         Duration durationToWaitForAllHandlersToFinishBeforeShutdown) {
//        this.errorReporter = errorReporter;
//        if (operations.hasNext()) {
//            this.uniformWindowedOperationStreamExecutorServiceThread = new UniformWindowedOperationStreamExecutorServiceThread(
//                    timeSource,
//                    firstWindowStartTime,
//                    windowSize,
//                    operationHandlerExecutor,
//                    errorReporter,
//                    operations,
//                    hasFinished,
//                    spinner,
//                    forceThreadToTerminate,
//                    db,
//                    operationClassifications,
//                    localCompletionTimeWriter,
//                    globalCompletionTimeReader,
//                    metricsService,
//                    durationToWaitForAllHandlersToFinishBeforeShutdown);
//        } else {
//            this.uniformWindowedOperationStreamExecutorServiceThread = null;
//            executing.set(true);
//            hasFinished.set(true);
//            shutdown.set(false);
//        }
//    }
//
//    synchronized public AtomicBoolean execute() {
//        if (executing.get())
//            return hasFinished;
//        executing.set(true);
//        uniformWindowedOperationStreamExecutorServiceThread.start();
//        return hasFinished;
//    }
//
//    synchronized public void shutdown() throws OperationHandlerExecutorException {
//        if (shutdown.get())
//            throw new OperationHandlerExecutorException("Executor has already been shutdown");
//        if (null != uniformWindowedOperationStreamExecutorServiceThread)
//            doShutdown();
//        shutdown.set(true);
//    }
//
//    private void doShutdown() {
//        try {
//            forceThreadToTerminate.set(true);
//            uniformWindowedOperationStreamExecutorServiceThread.join(SHUTDOWN_WAIT_TIMEOUT.asMilli());
//        } catch (Exception e) {
//            String errMsg = String.format("Unexpected error encountered while shutting down thread\n%s",
//                    ConcurrentErrorReporter.stackTraceToString(e));
//            errorReporter.reportError(this, errMsg);
//        }
//    }
}
