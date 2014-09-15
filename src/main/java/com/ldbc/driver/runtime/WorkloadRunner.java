package com.ldbc.driver.runtime;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.executor.*;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.runtime.streams.IteratorSplitter;
import com.ldbc.driver.runtime.streams.IteratorSplittingException;
import com.ldbc.driver.runtime.streams.SplitDefinition;
import com.ldbc.driver.runtime.streams.SplitResult;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ldbc.driver.OperationClassification.SchedulingMode;

public class WorkloadRunner {
    public static final Duration EARLY_SPINNER_OFFSET_DURATION = Duration.fromMilli(100);
    public static final long RUNNER_POLLING_INTERVAL_AS_MILLI = Duration.fromMilli(100).asMilli();
    private static final Duration WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN = Duration.fromSeconds(5);
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final TimeSource TIME_SOURCE;

    private final Spinner exactSpinner;
    private final Spinner slightlyEarlySpinner;

    // TODO make service and inject into workload runner. this could report to coordinator OR a local console printer, for example
    private WorkloadStatusThread workloadStatusThread;

    private final ConcurrentErrorReporter errorReporter;

    private final OperationHandlerExecutor threadPoolForWindowed;
    private final OperationHandlerExecutor threadPoolForBlocking;
    private final OperationHandlerExecutor threadPoolForAsynchronous;

    private final PreciseIndividualAsyncOperationStreamExecutorService preciseIndividualAsyncOperationStreamExecutorService;
    private final PreciseIndividualBlockingOperationStreamExecutorService preciseIndividualBlockingOperationStreamExecutorService;
    private final UniformWindowedOperationStreamExecutorService uniformWindowedOperationStreamExecutorService;

    private final Duration statusDisplayInterval;

    public WorkloadRunner(TimeSource timeSource,
                          Db db,
                          Iterator<Operation<?>> operations,
                          final Map<Class<? extends Operation>, OperationClassification> operationClassifications,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          ConcurrentCompletionTimeService completionTimeService,
                          int threadCount,
                          Duration statusDisplayInterval,
                          Time workloadStartTime,
                          Duration toleratedExecutionDelayDuration,
                          Duration spinnerSleepDuration,
                          Duration executionWindowDuration,
                          Duration earlySpinnerOffsetDuration) throws WorkloadException {
        this.TIME_SOURCE = timeSource;
        this.errorReporter = errorReporter;
        this.statusDisplayInterval = statusDisplayInterval;

        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                TIME_SOURCE,
                toleratedExecutionDelayDuration,
                errorReporter);

        // TODO for the spinner sent to Window scheduler allow delay to reach to the end of window?

        this.exactSpinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy);
        Duration earlySpinnerSleepDuration = Duration.fromMilli(Math.max(earlySpinnerOffsetDuration.asMilli() / 2, spinnerSleepDuration.asMilli()));
        this.slightlyEarlySpinner = new Spinner(TIME_SOURCE, earlySpinnerSleepDuration, executionDelayPolicy, earlySpinnerOffsetDuration);
        // TODO make this a configuration parameter?
        boolean detailedStatus = true;
        if (statusDisplayInterval.asSeconds() > 0)
            this.workloadStatusThread = new WorkloadStatusThread(
                    statusDisplayInterval,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    detailedStatus);

        Predicate<Class<Operation<?>>> isWriteOperationType = new Predicate<Class<Operation<?>>>() {
            @Override
            public boolean apply(Class<Operation<?>> operationType) {
                return true == operationClassifications.get(operationType).dependencyMode().equals(OperationClassification.DependencyMode.READ_WRITE);
            }
        };
        Predicate<Class<Operation<?>>> isReadOperationType = new Predicate<Class<Operation<?>>>() {
            @Override
            public boolean apply(Class<Operation<?>> operationType) {
                return false == operationClassifications.get(operationType).dependencyMode().equals(OperationClassification.DependencyMode.READ_WRITE);
            }
        };

        // TODO separate into read and write later
        List<Class<Operation<?>>> windowedOperationTypes =
                Lists.newArrayList(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.WINDOWED));
        // TODO separate into read and write later
        List<Class<Operation<?>>> blockingOperationTypes =
                Lists.newArrayList(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_BLOCKING));
        List<Class<Operation<?>>> asyncWriteOperationTypes = Lists.newArrayList(Iterables.filter(
                Lists.newArrayList(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_ASYNC)),
                isWriteOperationType
        ));
        List<Class<Operation<?>>> asyncReadOperationTypes = Lists.newArrayList(Iterables.filter(
                Lists.newArrayList(Workload.operationTypesBySchedulingMode(operationClassifications, SchedulingMode.INDIVIDUAL_ASYNC)),
                isReadOperationType
        ));

        Class<Operation<?>>[] windowedOperationTypesArray = windowedOperationTypes.toArray(new Class[windowedOperationTypes.size()]);
        Class<Operation<?>>[] blockingOperationTypesArray = blockingOperationTypes.toArray(new Class[blockingOperationTypes.size()]);
        Class<Operation<?>>[] asyncWriteOperationTypesArray = asyncWriteOperationTypes.toArray(new Class[asyncWriteOperationTypes.size()]);
        Class<Operation<?>>[] asyncReadOperationTypesArray = asyncReadOperationTypes.toArray(new Class[asyncReadOperationTypes.size()]);

        List<Operation<?>> windowedOperations;
        List<Operation<?>> blockingOperations;
        List<Operation<?>> asynchronousWriteOperations;
        List<Operation<?>> asynchronousReadOperations;
        try {
            IteratorSplitter<Operation<?>> splitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);

            SplitDefinition<Operation<?>> windowed = new SplitDefinition<>(windowedOperationTypesArray);
            SplitDefinition<Operation<?>> blocking = new SplitDefinition<>(blockingOperationTypesArray);
            SplitDefinition<Operation<?>> asynchronousWrite = new SplitDefinition<>(asyncWriteOperationTypesArray);
            SplitDefinition<Operation<?>> asynchronousRead = new SplitDefinition<>(asyncReadOperationTypesArray);

            SplitResult splits = splitter.split(operations, windowed, blocking, asynchronousWrite, asynchronousRead);
            windowedOperations = splits.getSplitFor(windowed);
            blockingOperations = splits.getSplitFor(blocking);
            asynchronousWriteOperations = splits.getSplitFor(asynchronousWrite);
            asynchronousReadOperations = splits.getSplitFor(asynchronousRead);
        } catch (IteratorSplittingException e) {
            throw new WorkloadException(
                    String.format("Error while splitting operation stream by scheduling mode\n%s", ConcurrentErrorReporter.stackTraceToString(e)),
                    e);
        }

        Predicate<Operation<?>> isReadWriteOperation = new Predicate<Operation<?>>() {
            @Override
            public boolean apply(Operation<?> operation) {
                if (operationClassifications.containsKey(operation.getClass()))
                    return operationClassifications.get(operation.getClass()).dependencyMode().equals(OperationClassification.DependencyMode.READ_WRITE);
                else
                    return false;
            }
        };

        // only create a local completion time writer for an executor if it contains at least one READ_WRITE operation
        // otherwise it will cause completion time to stall
        LocalCompletionTimeWriter localCompletionTimeWriterForAsynchronous;
        LocalCompletionTimeWriter localCompletionTimeWriterForBlocking;
        LocalCompletionTimeWriter localCompletionTimeWriterForWindowed;
        try {
            localCompletionTimeWriterForAsynchronous = (asynchronousWriteOperations.isEmpty())
                    ? DUMMY_LOCAL_COMPLETION_TIME_WRITER
                    : completionTimeService.newLocalCompletionTimeWriter();
            localCompletionTimeWriterForBlocking = (Iterables.any(blockingOperations, isReadWriteOperation))
                    ? completionTimeService.newLocalCompletionTimeWriter()
                    : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
            localCompletionTimeWriterForWindowed = (Iterables.any(windowedOperations, isReadWriteOperation))
                    ? completionTimeService.newLocalCompletionTimeWriter()
                    : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
        } catch (CompletionTimeException e) {
            throw new WorkloadException("Error while attempting to create local completion time writer", e);
        }

        // Thread pools
        // TODO get thread counts from config, or in more intelligent manner
        // TODO move thread pool creation into executor services so workload runner does not have to know about them
        // TODO calculate thread pool sizes
        this.threadPoolForWindowed = new ThreadPoolOperationHandlerExecutor(threadCount);
        this.threadPoolForBlocking = new SingleThreadOperationHandlerExecutor(errorReporter);
        this.threadPoolForAsynchronous = new ThreadPoolOperationHandlerExecutor(threadCount);

        // Executors
        this.preciseIndividualAsyncOperationStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(
                TIME_SOURCE,
                errorReporter,
                asynchronousReadOperations.iterator(),
                asynchronousWriteOperations.iterator(),
                exactSpinner,
                slightlyEarlySpinner,
                threadPoolForAsynchronous,
                operationClassifications,
                db,
                localCompletionTimeWriterForAsynchronous,
                completionTimeService,
                metricsService);
        this.preciseIndividualBlockingOperationStreamExecutorService = new PreciseIndividualBlockingOperationStreamExecutorService(
                TIME_SOURCE,
                errorReporter,
                blockingOperations.iterator(),
                exactSpinner,
                slightlyEarlySpinner,
                threadPoolForBlocking,
                operationClassifications,
                db,
                localCompletionTimeWriterForBlocking,
                completionTimeService,
                metricsService);
        this.uniformWindowedOperationStreamExecutorService = new UniformWindowedOperationStreamExecutorService(
                TIME_SOURCE,
                errorReporter,
                windowedOperations.iterator(),
                threadPoolForWindowed,
                exactSpinner,
                slightlyEarlySpinner,
                workloadStartTime,
                executionWindowDuration,
                db,
                operationClassifications,
                localCompletionTimeWriterForWindowed,
                completionTimeService,
                metricsService);
    }

    // TODO executeWorkload should return a result (e.g., Success/Fail, and ErrorType if Fail)
    // TODO and then it does not need to throw an exception
    public void executeWorkload() throws WorkloadException {
        if (statusDisplayInterval.asSeconds() > 0)
            workloadStatusThread.start();

        AtomicBoolean asyncHandlersFinished = preciseIndividualAsyncOperationStreamExecutorService.execute();
        AtomicBoolean blockingHandlersFinished = preciseIndividualBlockingOperationStreamExecutorService.execute();
        AtomicBoolean windowedHandlersFinished = uniformWindowedOperationStreamExecutorService.execute();

        while (true) {
            // Error encountered in one or more of the worker threads --> terminate run
            if (errorReporter.errorEncountered()) {
                boolean forced = true;
                String shutdownErrMsg = shutdownEverything(forced);
                throw new WorkloadException(String.format("%s\nError encountered while running workload\n%s",
                        shutdownErrMsg,
                        errorReporter.toString()));
            }

            // All executors have completed --> return
            if (asyncHandlersFinished.get() && blockingHandlersFinished.get() && windowedHandlersFinished.get())
                break;

            // Take short break between error & completion checks to reduce CPU utilization
            Spinner.powerNap(RUNNER_POLLING_INTERVAL_AS_MILLI);
        }

        // One last check for errors encountered in any of the worker threads --> terminate run
        if (errorReporter.errorEncountered()) {
            boolean forced = true;
            String showdownErrMsg = shutdownEverything(forced);
            throw new WorkloadException(String.format("%sEncountered error while running workload. Driver terminating.\n%s",
                    showdownErrMsg,
                    errorReporter.toString()));
        }

        boolean forced = false;
        String shutdownErrMsg = shutdownEverything(forced);

        if (false == "".equals(shutdownErrMsg)) {
            throw new WorkloadException(shutdownErrMsg);
        }
    }

    private String shutdownEverything(boolean forced) {
        String errMsg = "";

        try {
            preciseIndividualAsyncOperationStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    preciseIndividualAsyncOperationStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        try {
            preciseIndividualBlockingOperationStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    preciseIndividualBlockingOperationStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        try {
            uniformWindowedOperationStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    uniformWindowedOperationStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        try {
            if (forced) {
                // if forced shutdown (error) some handlers likely still running,
                // but for now it does not matter as the process will terminate anyway
                // (though when running test suite it can result in many running threads, making the tests much slower)
                threadPoolForAsynchronous.shutdown(Duration.fromMilli(0));
                threadPoolForBlocking.shutdown(Duration.fromMilli(0));
                threadPoolForWindowed.shutdown(Duration.fromMilli(0));
            } else {
                // if normal shutdown all executors have completed by this stage
                threadPoolForAsynchronous.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
                threadPoolForBlocking.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
                threadPoolForWindowed.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
            }
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down\n%s",
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        if (statusDisplayInterval.asSeconds() > 0) {
            workloadStatusThread.shutdown();
            workloadStatusThread.interrupt();
        }

        return errMsg;
    }
}
