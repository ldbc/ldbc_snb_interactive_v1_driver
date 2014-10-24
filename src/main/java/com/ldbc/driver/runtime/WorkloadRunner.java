package com.ldbc.driver.runtime;

import com.ldbc.driver.Db;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.executor.*;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;

public class WorkloadRunner {
    public static final Duration DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH = Duration.fromMinutes(180);
    public static final long RUNNER_POLLING_INTERVAL_AS_MILLI = Duration.fromMilli(100).asMilli();
    private static final Duration WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN = Duration.fromSeconds(5);
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final Spinner exactSpinner;

    // TODO make service and inject into workload runner. this could report to coordinator OR a local console printer, for example
    private WorkloadStatusThread workloadStatusThread;

    private final ConcurrentErrorReporter errorReporter;

    private final OperationHandlerExecutor executorForAsynchronous;
    private final List<OperationHandlerExecutor> executorsForBlocking = new ArrayList<>();

    private final PreciseIndividualAsyncOperationStreamExecutorService asynchronousStreamExecutorService;
    private final List<PreciseIndividualBlockingOperationStreamExecutorService> blockingStreamExecutorServices = new ArrayList<>();

    private final Duration statusDisplayInterval;

    public WorkloadRunner(TimeSource timeSource,
                          Db db,
                          WorkloadStreams workloadStreams,
                          ConcurrentMetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          ConcurrentCompletionTimeService completionTimeService,
                          int threadCount,
                          Duration statusDisplayInterval,
                          Time workloadStartTime,
                          Duration spinnerSleepDuration,
                          Duration executionWindowDuration,
                          Duration durationToWaitForAllHandlersToFinishBeforeShutdown,
                          boolean ignoreScheduleStartTimes,
                          int operationHandlerExecutorsBoundedQueueSize) throws WorkloadException {
        this.errorReporter = errorReporter;
        this.statusDisplayInterval = statusDisplayInterval;

        this.exactSpinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduleStartTimes);

        boolean detailedStatus = true;
        if (statusDisplayInterval.asSeconds() > 0)
            this.workloadStatusThread = new WorkloadStatusThread(
                    statusDisplayInterval,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    detailedStatus);

        // only create a local completion time writer for an executor if it contains at least one READ_WRITE operation
        // otherwise it will cause completion time to stall
        WorkloadStreamDefinition asynchronousStream = workloadStreams.asynchronousStream();
        this.executorForAsynchronous = new ThreadPoolOperationHandlerExecutor(threadCount, operationHandlerExecutorsBoundedQueueSize);
        LocalCompletionTimeWriter localCompletionTimeWriterForAsynchronous;
        try {
            localCompletionTimeWriterForAsynchronous = (asynchronousStream.dependencyOperations().hasNext())
                    ? completionTimeService.newLocalCompletionTimeWriter()
                    : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
        } catch (CompletionTimeException e) {
            throw new WorkloadException("Error while attempting to create local completion time writer", e);
        }
        this.asynchronousStreamExecutorService = new PreciseIndividualAsyncOperationStreamExecutorService(
                timeSource,
                errorReporter,
                asynchronousStream,
                exactSpinner,
                executorForAsynchronous,
                db,
                localCompletionTimeWriterForAsynchronous,
                completionTimeService,
                metricsService,
                durationToWaitForAllHandlersToFinishBeforeShutdown);

        for (WorkloadStreamDefinition blockingStream : workloadStreams.blockingStreamDefinitions()) {
            // TODO benchmark more to find out which policy is best Same Thread vs Single Thread
//            OperationHandlerExecutor executorForBlocking = new SameThreadOperationHandlerExecutor();
            OperationHandlerExecutor executorForBlocking = new SingleThreadOperationHandlerExecutor(errorReporter, operationHandlerExecutorsBoundedQueueSize);
            this.executorsForBlocking.add(executorForBlocking);
            // only create a local completion time writer for an executor if it contains at least one READ_WRITE operation
            // otherwise it will cause completion time to stall
            LocalCompletionTimeWriter localCompletionTimeWriterForBlocking;
            try {
                localCompletionTimeWriterForBlocking = (blockingStream.dependencyOperations().hasNext())
                        ? completionTimeService.newLocalCompletionTimeWriter()
                        : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
            } catch (CompletionTimeException e) {
                throw new WorkloadException("Error while attempting to create local completion time writer", e);
            }
            this.blockingStreamExecutorServices.add(
                    new PreciseIndividualBlockingOperationStreamExecutorService(
                            timeSource,
                            errorReporter,
                            blockingStream,
                            exactSpinner,
                            executorForBlocking,
                            db,
                            localCompletionTimeWriterForBlocking,
                            completionTimeService,
                            metricsService,
                            durationToWaitForAllHandlersToFinishBeforeShutdown)
            );
        }
    }

    // TODO executeWorkload should return a result (e.g., Success/Fail, and ErrorType if Fail)
    // TODO and then it does not need to throw an exception
    public void executeWorkload() throws WorkloadException {
        if (statusDisplayInterval.asSeconds() > 0)
            workloadStatusThread.start();

        AtomicBoolean[] executorFinishedFlags = new AtomicBoolean[blockingStreamExecutorServices.size() + 1];
        executorFinishedFlags[0] = asynchronousStreamExecutorService.execute();
        for (int i = 0; i < blockingStreamExecutorServices.size(); i++) {
            executorFinishedFlags[i + 1] = blockingStreamExecutorServices.get(i).execute();
        }

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
            boolean allExecutorsHaveCompleted = true;
            for (int i = 0; i < executorFinishedFlags.length; i++) {
                if (false == executorFinishedFlags[i].get()) {
                    allExecutorsHaveCompleted = false;
                    break;
                }
            }
            if (allExecutorsHaveCompleted) break;

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
            asynchronousStreamExecutorService.shutdown();
        } catch (OperationHandlerExecutorException e) {
            errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                    asynchronousStreamExecutorService.getClass().getSimpleName(),
                    ConcurrentErrorReporter.stackTraceToString(e));
        }

        for (PreciseIndividualBlockingOperationStreamExecutorService blockingStreamExecutorService : blockingStreamExecutorServices) {
            try {
                blockingStreamExecutorService.shutdown();
            } catch (OperationHandlerExecutorException e) {
                errMsg += String.format("Encountered error while shutting down %s\n%s\n",
                        blockingStreamExecutorService.getClass().getSimpleName(),
                        ConcurrentErrorReporter.stackTraceToString(e));
            }
        }

        try {
            if (forced) {
                // if forced shutdown (error) some handlers likely still running,
                // but for now it does not matter as the process will terminate anyway
                // (though when running test suite it can result in many running threads, making the tests much slower)
                executorForAsynchronous.shutdown(Duration.fromMilli(0));
                for (OperationHandlerExecutor executorForBlocking : executorsForBlocking) {
                    executorForBlocking.shutdown(Duration.fromMilli(0));
                }
            } else {
                // if normal shutdown all executors have completed by this stage
                executorForAsynchronous.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
                for (OperationHandlerExecutor executorForBlocking : executorsForBlocking) {
                    executorForBlocking.shutdown(WAIT_DURATION_FOR_OPERATION_HANDLER_EXECUTOR_TO_SHUTDOWN);
                }
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
