package com.ldbc.driver.runtime;

import com.ldbc.driver.Db;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeService;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.executor.*;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;

public class WorkloadRunner {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    public static final long RUNNER_POLLING_INTERVAL_AS_MILLI = 100;
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER = new DummyLocalCompletionTimeWriter();

    private final Spinner spinner;

    // TODO make service and inject into workload runner. this could report to coordinator OR a local console printer, for example
    private WorkloadStatusThread workloadStatusThread;

    private final ConcurrentErrorReporter errorReporter;

    private final OperationExecutor executorForAsynchronous;
    private final List<OperationExecutor> executorsForBlocking = new ArrayList<>();

    private final OperationStreamExecutorService asynchronousStreamExecutorService;
    private final List<OperationStreamExecutorService> blockingStreamExecutorServices = new ArrayList<>();

    private final long statusDisplayIntervalAsMilli;

    public WorkloadRunner(TimeSource timeSource,
                          Db db,
                          WorkloadStreams workloadStreams,
                          MetricsService metricsService,
                          ConcurrentErrorReporter errorReporter,
                          CompletionTimeService completionTimeService,
                          int threadCount,
                          long statusDisplayIntervalAsSeconds,
                          long spinnerSleepDurationAsMilli,
                          boolean ignoreScheduleStartTimes,
                          int operationHandlerExecutorsBoundedQueueSize) throws WorkloadException, MetricsCollectionException {
        this.errorReporter = errorReporter;
        this.statusDisplayIntervalAsMilli = statusDisplayIntervalAsSeconds;

        this.spinner = new Spinner(timeSource, spinnerSleepDurationAsMilli, ignoreScheduleStartTimes);

        boolean detailedStatus = false;
        if (statusDisplayIntervalAsSeconds > 0) {
            this.workloadStatusThread = new WorkloadStatusThread(
                    TimeUnit.SECONDS.toMillis(statusDisplayIntervalAsSeconds),
                    metricsService.getWriter(),
                    errorReporter,
                    completionTimeService,
                    detailedStatus
            );
        }
        // only create a local completion time writer for an executor if it contains at least one READ_WRITE operation
        // otherwise it will cause completion time to stall
        WorkloadStreamDefinition asynchronousStream = workloadStreams.asynchronousStream();
        LocalCompletionTimeWriter localCompletionTimeWriterForAsynchronous;
        try {
            localCompletionTimeWriterForAsynchronous = (asynchronousStream.dependencyOperations().hasNext())
                    ? completionTimeService.newLocalCompletionTimeWriter()
                    : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
        } catch (CompletionTimeException e) {
            throw new WorkloadException("Error while attempting to create local completion time writer", e);
        }
        this.executorForAsynchronous = new ThreadPoolOperationExecutor(
                threadCount,
                operationHandlerExecutorsBoundedQueueSize,
                db,
                asynchronousStream,
                localCompletionTimeWriterForAsynchronous,
                completionTimeService,
                spinner,
                timeSource,
                errorReporter,
                metricsService,
                asynchronousStream.childOperationGenerator()
        );
        this.asynchronousStreamExecutorService = new OperationStreamExecutorService(
                errorReporter,
                asynchronousStream,
                executorForAsynchronous,
                localCompletionTimeWriterForAsynchronous
        );

        for (WorkloadStreamDefinition blockingStream : workloadStreams.blockingStreamDefinitions()) {
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
            // TODO benchmark more to find out which policy is best Same Thread vs Single Thread
            OperationExecutor executorForBlocking = new SameThreadOperationExecutor(
                    db,
                    blockingStream,
                    localCompletionTimeWriterForBlocking,
                    completionTimeService,
                    spinner,
                    timeSource,
                    errorReporter,
                    metricsService,
                    blockingStream.childOperationGenerator()
            );
            this.executorsForBlocking.add(executorForBlocking);
            this.blockingStreamExecutorServices.add(
                    new OperationStreamExecutorService(
                            errorReporter,
                            blockingStream,
                            executorForBlocking,
                            localCompletionTimeWriterForBlocking
                    )
            );
        }
    }

    // TODO executeWorkload should return a result (e.g., Success/Fail, and ErrorType if Fail)
    // TODO and then it does not need to throw an exception
    public void executeWorkload() throws WorkloadException {
        if (statusDisplayIntervalAsMilli > 0)
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
                shutdownEverything(forced, errorReporter);
                throw new WorkloadException(String.format("Error encountered while running workload\n%s",
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
            shutdownEverything(forced, errorReporter);
            throw new WorkloadException(String.format("Encountered error while running workload. Driver terminating.\n%s",
                    errorReporter.toString()));
        }

        boolean forced = false;
        shutdownEverything(forced, errorReporter);

        if (errorReporter.errorEncountered()) {
            throw new WorkloadException(errorReporter.toString());
        }
    }

    private void shutdownEverything(boolean forced, ConcurrentErrorReporter errorReporter) {
        // if forced shutdown (error) some handlers likely still running,
        // but for now it does not matter as the process will terminate anyway
        // (though when running test suite it can result in many running threads, making the tests much slower)
        //
        // if normal shutdown all executors have completed by this stage
        long shutdownWait = (forced) ? 1 : OperationStreamExecutorService.SHUTDOWN_WAIT_TIMEOUT_AS_MILLI;

        try {
            asynchronousStreamExecutorService.shutdown(shutdownWait);
        } catch (OperationExecutorException e) {
            errorReporter.reportError(
                    this,
                    String.format("Encountered error while shutting down %s\n%s\n",
                            asynchronousStreamExecutorService.getClass().getSimpleName(),
                            ConcurrentErrorReporter.stackTraceToString(e))
            );
        }

        for (OperationStreamExecutorService blockingStreamExecutorService : blockingStreamExecutorServices) {
            try {
                blockingStreamExecutorService.shutdown(shutdownWait);
            } catch (OperationExecutorException e) {
                errorReporter.reportError(
                        this,
                        String.format("Encountered error while shutting down %s\n%s\n",
                                blockingStreamExecutorService.getClass().getSimpleName(),
                                ConcurrentErrorReporter.stackTraceToString(e))
                );
            }
        }

        try {
            // if forced shutdown (error) some handlers likely still running,
            // but for now it does not matter as the process will terminate anyway
            // (though when running test suite it can result in many running threads, making the tests much slower)
            executorForAsynchronous.shutdown(shutdownWait);
            for (OperationExecutor executorForBlocking : executorsForBlocking) {
                executorForBlocking.shutdown(shutdownWait);
            }
        } catch (OperationExecutorException e) {
            errorReporter.reportError(
                    this,
                    String.format("Encountered error while shutting down\n%s",
                            ConcurrentErrorReporter.stackTraceToString(e))
            );
        }

        if (statusDisplayIntervalAsMilli > 0) {
            System.out.println("Shutting down status thread...");
            workloadStatusThread.shutdown();
            workloadStatusThread.interrupt();
            try {
                workloadStatusThread.join();
            } catch (InterruptedException e) {
                // do nothing
            }
        }
    }
}