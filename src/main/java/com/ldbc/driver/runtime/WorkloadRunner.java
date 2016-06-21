package com.ldbc.driver.runtime;

import com.ldbc.driver.Db;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeService;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.executor.OperationExecutor;
import com.ldbc.driver.runtime.executor.OperationExecutorException;
import com.ldbc.driver.runtime.executor.OperationStreamExecutorService;
import com.ldbc.driver.runtime.executor.SameThreadOperationExecutor;
import com.ldbc.driver.runtime.executor.ThreadPoolOperationExecutor;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.ldbc.driver.WorkloadStreams.WorkloadStreamDefinition;
import static java.lang.String.format;

public class WorkloadRunner
{
    public static final long RUNNER_POLLING_INTERVAL_AS_MILLI = 100;
    private static final LocalCompletionTimeWriter DUMMY_LOCAL_COMPLETION_TIME_WRITER =
            new DummyLocalCompletionTimeWriter();

    private final WorkloadRunnerFuture workloadRunnerFuture;

    public WorkloadRunner(
            TimeSource timeSource,
            Db db,
            WorkloadStreams workloadStreams,
            MetricsService metricsService,
            ConcurrentErrorReporter errorReporter,
            CompletionTimeService completionTimeService,
            LoggingServiceFactory loggingServiceFactory,
            int threadCount,
            long statusDisplayIntervalAsSeconds,
            long spinnerSleepDurationAsMilli,
            boolean ignoreScheduleStartTimes,
            int operationHandlerExecutorsBoundedQueueSize ) throws WorkloadException, MetricsCollectionException
    {
        this.workloadRunnerFuture = new WorkloadRunnerFuture(
                timeSource,
                db,
                workloadStreams,
                metricsService,
                errorReporter,
                completionTimeService,
                loggingServiceFactory,
                threadCount,
                statusDisplayIntervalAsSeconds,
                spinnerSleepDurationAsMilli,
                ignoreScheduleStartTimes,
                operationHandlerExecutorsBoundedQueueSize
        );
    }

    public Future<ConcurrentErrorReporter> getFuture()
    {
        workloadRunnerFuture.startThread();
        return workloadRunnerFuture;
    }

    private static class WorkloadRunnerFuture implements Future<ConcurrentErrorReporter>
    {
        private final WorkloadRunnerThread workloadRunnerThread;
        private final TimeSource timeSource;
        private final ConcurrentErrorReporter errorReporter;
        private boolean isCancelled = false;
        private boolean isDone = false;

        private WorkloadRunnerFuture(
                TimeSource timeSource,
                Db db,
                WorkloadStreams workloadStreams,
                MetricsService metricsService,
                ConcurrentErrorReporter errorReporter,
                CompletionTimeService completionTimeService,
                LoggingServiceFactory loggingServiceFactory,
                int threadCount,
                long statusDisplayIntervalAsSeconds,
                long spinnerSleepDurationAsMilli,
                boolean ignoreScheduleStartTimes,
                int operationHandlerExecutorsBoundedQueueSize ) throws MetricsCollectionException, WorkloadException
        {
            this.workloadRunnerThread = new WorkloadRunnerThread(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    loggingServiceFactory,
                    threadCount,
                    statusDisplayIntervalAsSeconds,
                    spinnerSleepDurationAsMilli,
                    ignoreScheduleStartTimes,
                    operationHandlerExecutorsBoundedQueueSize
            );
            this.timeSource = timeSource;
            this.errorReporter = errorReporter;
        }

        private void startThread()
        {
            if ( workloadRunnerThread.state().equals( WorkloadRunnerThreadState.NOT_STARTED ) )
            {
                workloadRunnerThread.start();
                while ( workloadRunnerThread.state().equals( WorkloadRunnerThreadState.NOT_STARTED ) )
                {
                    Spinner.powerNap( RUNNER_POLLING_INTERVAL_AS_MILLI );
                }
            }
        }

        @Override
        public boolean cancel( boolean mayInterruptIfRunning )
        {
            // After this method returns, subsequent calls to isDone will always return true
            // Subsequent calls to isCancelled will always return true if this method returned true

            switch ( workloadRunnerThread.state() )
            {
            case NOT_STARTED:
                // Does not make sense to terminate a task that has not yet started
                // This task should never run
                isDone = true;
                return false;
            case RUNNING:
                if ( isCancelled )
                {
                    // Fail because task has already been cancelled
                    return false;
                }
                else
                {
                    isCancelled = true;
                    errorReporter.reportError( this, "Workload execution was manually terminated" );
                    try
                    {
                        waitForCompletion( Long.MAX_VALUE );
                    }
                    catch ( TimeoutException e )
                    {
                        // do nothing
                    }
                    isDone = true;
                    return isCancelled;
                }
            case COMPLETED_SUCCEEDED:
                // Fail because task has already completed
                return false;
            case COMPLETED_FAILED:
                // Fail because task has already completed
                return false;
            default:
                // Fail because task has already completed
                throw new IllegalStateException(
                        format( "Unrecognized %s: %s",
                                workloadRunnerThread.state().getClass().getSimpleName(),
                                workloadRunnerThread.state() )
                );
            }
        }

        @Override
        public boolean isCancelled()
        {
            return isCancelled;
        }

        @Override
        public boolean isDone()
        {
            if ( !isDone )
            {
                if ( workloadRunnerThread.state().equals( WorkloadRunnerThreadState.COMPLETED_FAILED ) ||
                     workloadRunnerThread.state().equals( WorkloadRunnerThreadState.COMPLETED_SUCCEEDED ) )
                {
                    isDone = true;
                }
            }
            return isDone;
        }

        @Override
        public ConcurrentErrorReporter get() throws InterruptedException, ExecutionException
        {
            if ( isCancelled || isDone )
            {
                throw new IllegalStateException( "Can not call method after future has been cancelled or completed" );
            }
            try
            {
                waitForCompletion( Long.MAX_VALUE );
            }
            catch ( TimeoutException e )
            {
                // do nothing
            }
            return errorReporter;

        }

        @Override
        public ConcurrentErrorReporter get( long timeout, TimeUnit unit )
                throws InterruptedException, ExecutionException, TimeoutException
        {
            if ( isCancelled || isDone )
            {
                throw new IllegalStateException( "Can not call method after future has been cancelled or completed" );
            }
            long waitDurationMs = unit.toMillis( timeout );
            waitForCompletion( waitDurationMs );
            return errorReporter;
        }

        private void waitForCompletion( long waitDurationMs ) throws TimeoutException
        {
            long startTimeMs = timeSource.nowAsMilli();
            while ( timeSource.nowAsMilli() - startTimeMs < waitDurationMs )
            {
                switch ( workloadRunnerThread.state() )
                {
                case NOT_STARTED:
                    throw new IllegalStateException( format(
                            "%s is in %s state, but should have already started",
                            WorkloadRunnerThread.class.getSimpleName(),
                            WorkloadRunnerThreadState.NOT_STARTED.name()
                    ) );
                case RUNNING:
                    Spinner.powerNap( RUNNER_POLLING_INTERVAL_AS_MILLI );
                    continue;
                case COMPLETED_SUCCEEDED:
                    return;
                case COMPLETED_FAILED:
                    return;
                default:
                    throw new IllegalStateException( format(
                            "Unknown %s: %s",
                            WorkloadRunnerThreadState.class.getSimpleName(),
                            WorkloadRunnerThreadState.NOT_STARTED.name()
                    ) );
                }
            }
            throw new TimeoutException( "Workload execution did not complete in time" );
        }
    }

    private enum WorkloadRunnerThreadState
    {
        NOT_STARTED,
        RUNNING,
        COMPLETED_SUCCEEDED,
        COMPLETED_FAILED
    }

    private static class WorkloadRunnerThread extends Thread
    {
        private final Spinner spinner;
        private WorkloadStatusThread workloadStatusThread;
        private final ConcurrentErrorReporter errorReporter;
        private final OperationExecutor executorForAsynchronous;
        private final List<OperationExecutor> executorsForBlocking = new ArrayList<>();
        private final OperationStreamExecutorService asynchronousStreamExecutorService;
        private final List<OperationStreamExecutorService> blockingStreamExecutorServices = new ArrayList<>();
        private final long statusDisplayIntervalAsMilli;
        private final AtomicReference<WorkloadRunnerThreadState> stateRef;

        private enum ShutdownType
        {
            NORMAL,
            FORCED
        }

        public WorkloadRunnerThread( TimeSource timeSource,
                Db db,
                WorkloadStreams workloadStreams,
                MetricsService metricsService,
                ConcurrentErrorReporter errorReporter,
                CompletionTimeService completionTimeService,
                LoggingServiceFactory loggingServiceFactory,
                int threadCount,
                long statusDisplayIntervalAsSeconds,
                long spinnerSleepDurationAsMilli,
                boolean ignoreScheduleStartTimes,
                int operationHandlerExecutorsBoundedQueueSize ) throws WorkloadException, MetricsCollectionException
        {
            this.errorReporter = errorReporter;
            this.statusDisplayIntervalAsMilli = statusDisplayIntervalAsSeconds;

            this.spinner = new Spinner( timeSource, spinnerSleepDurationAsMilli, ignoreScheduleStartTimes );

            if ( statusDisplayIntervalAsSeconds > 0 )
            {
                this.workloadStatusThread = new WorkloadStatusThread(
                        TimeUnit.SECONDS.toMillis( statusDisplayIntervalAsSeconds ),
                        metricsService.getWriter(),
                        errorReporter,
                        completionTimeService,
                        loggingServiceFactory
                );
            }
            // only create a local completion time writer for an executor if it contains at least one READ_WRITE
            // operation
            // otherwise it will cause completion time to stall
            WorkloadStreamDefinition asynchronousStream = workloadStreams.asynchronousStream();
            LocalCompletionTimeWriter localCompletionTimeWriterForAsynchronous;
            try
            {
                localCompletionTimeWriterForAsynchronous = (asynchronousStream.dependencyOperations().hasNext())
                                                           ? completionTimeService.newLocalCompletionTimeWriter()
                                                           : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
            }
            catch ( CompletionTimeException e )
            {
                throw new WorkloadException( "Error while attempting to create local completion time writer", e );
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

            for ( WorkloadStreamDefinition blockingStream : workloadStreams.blockingStreamDefinitions() )
            {
                // only create a local completion time writer for an executor if it contains at least one READ_WRITE
                // operation
                // otherwise it will cause completion time to stall
                LocalCompletionTimeWriter localCompletionTimeWriterForBlocking;
                try
                {
                    localCompletionTimeWriterForBlocking = (blockingStream.dependencyOperations().hasNext())
                                                           ? completionTimeService.newLocalCompletionTimeWriter()
                                                           : DUMMY_LOCAL_COMPLETION_TIME_WRITER;
                }
                catch ( CompletionTimeException e )
                {
                    throw new WorkloadException( "Error while attempting to create local completion time writer", e );
                }
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
                this.executorsForBlocking.add( executorForBlocking );
                this.blockingStreamExecutorServices.add(
                        new OperationStreamExecutorService(
                                errorReporter,
                                blockingStream,
                                executorForBlocking,
                                localCompletionTimeWriterForBlocking
                        )
                );
            }
            this.stateRef = new AtomicReference<>( WorkloadRunnerThreadState.NOT_STARTED );
        }

        private WorkloadRunnerThreadState state()
        {
            return stateRef.get();
        }

        @Override
        public void run()
        {
            if ( statusDisplayIntervalAsMilli > 0 )
            {
                workloadStatusThread.start();
            }

            AtomicBoolean[] executorFinishedFlags = new AtomicBoolean[blockingStreamExecutorServices.size() + 1];
            executorFinishedFlags[0] = asynchronousStreamExecutorService.execute();
            for ( int i = 0; i < blockingStreamExecutorServices.size(); i++ )
            {
                executorFinishedFlags[i + 1] = blockingStreamExecutorServices.get( i ).execute();
            }

            stateRef.set( WorkloadRunnerThreadState.RUNNING );

            while ( true )
            {
                // Error encountered in one or more of the worker threads --> terminate run
                if ( errorReporter.errorEncountered() )
                {
                    shutdownEverything( ShutdownType.FORCED, errorReporter );
                    stateRef.set( WorkloadRunnerThreadState.COMPLETED_FAILED );
                    return;
                }

                // All executors have completed --> return
                boolean allExecutorsHaveCompleted = true;
                for ( int i = 0; i < executorFinishedFlags.length; i++ )
                {
                    if ( !executorFinishedFlags[i].get() )
                    {
                        allExecutorsHaveCompleted = false;
                        break;
                    }
                }
                if ( allExecutorsHaveCompleted )
                {
                    break;
                }

                // Take short break between error & completion checks to reduce CPU utilization
                Spinner.powerNap( RUNNER_POLLING_INTERVAL_AS_MILLI );
            }

            // One last check for errors encountered in any of the worker threads --> terminate run
            if ( errorReporter.errorEncountered() )
            {
                shutdownEverything( ShutdownType.FORCED, errorReporter );
                stateRef.set( WorkloadRunnerThreadState.COMPLETED_FAILED );
            }
            else
            {
                shutdownEverything( ShutdownType.NORMAL, errorReporter );
                if ( errorReporter.errorEncountered() )
                {
                    stateRef.set( WorkloadRunnerThreadState.COMPLETED_FAILED );
                }
                else
                {
                    stateRef.set( WorkloadRunnerThreadState.COMPLETED_SUCCEEDED );
                }
            }
        }

        private void shutdownEverything( ShutdownType shutdownType, ConcurrentErrorReporter errorReporter )
        {
            // if forced shutdown (error) some handlers likely still running,
            // but for now it does not matter as the process will terminate anyway
            // (though when running test suite it can result in many running threads, making the tests much slower)
            //
            // if normal shutdown all executors have completed by this stage
            long shutdownWait = (shutdownType.equals( ShutdownType.FORCED ))
                                ? 1
                                : OperationStreamExecutorService.SHUTDOWN_WAIT_TIMEOUT_AS_MILLI;

            try
            {
                asynchronousStreamExecutorService.shutdown( shutdownWait );
            }
            catch ( OperationExecutorException e )
            {
                errorReporter.reportError(
                        this,
                        format( "Encountered error while shutting down %s\n%s\n",
                                asynchronousStreamExecutorService.getClass().getSimpleName(),
                                ConcurrentErrorReporter.stackTraceToString( e ) )
                );
            }

            for ( OperationStreamExecutorService blockingStreamExecutorService : blockingStreamExecutorServices )
            {
                try
                {
                    blockingStreamExecutorService.shutdown( shutdownWait );
                }
                catch ( OperationExecutorException e )
                {
                    errorReporter.reportError(
                            this,
                            format( "Encountered error while shutting down %s\n%s\n",
                                    blockingStreamExecutorService.getClass().getSimpleName(),
                                    ConcurrentErrorReporter.stackTraceToString( e ) )
                    );
                }
            }

            try
            {
                // if forced shutdown (error) some handlers likely still running,
                // but for now it does not matter as the process will terminate anyway
                // (though when running test suite it can result in many running threads, making the tests much slower)
                executorForAsynchronous.shutdown( shutdownWait );
            }
            catch ( OperationExecutorException e )
            {
                errorReporter.reportError(
                        this,
                        format( "Encountered error while waiting for asynchronous executor to shutdown\n" +
                                "Handlers still running: %s\n" +
                                "%s",
                                executorForAsynchronous.uncompletedOperationHandlerCount(),
                                ConcurrentErrorReporter.stackTraceToString( e ) )
                );
            }

            try
            {
                // if forced shutdown (error) some handlers likely still running,
                // but for now it does not matter as the process will terminate anyway
                // (though when running test suite it can result in many running threads, making the tests much slower)
                for ( OperationExecutor executorForBlocking : executorsForBlocking )
                {
                    executorForBlocking.shutdown( shutdownWait );
                }
            }
            catch ( OperationExecutorException e )
            {
                long uncompletedOperationHandlerCount = 0;
                for ( OperationExecutor executorForBlocking : executorsForBlocking )
                {
                    uncompletedOperationHandlerCount += executorForBlocking.uncompletedOperationHandlerCount();
                }
                errorReporter.reportError(
                        this,
                        format( "Encountered error while waiting for a synchronous executor to shutdown\n" +
                                "Handlers still running: %s\n" +
                                "%s",
                                uncompletedOperationHandlerCount,
                                ConcurrentErrorReporter.stackTraceToString( e ) )
                );
            }

            if ( statusDisplayIntervalAsMilli > 0 )
            {
                System.out.println( "Shutting down status thread..." );
                workloadStatusThread.shutdown();
                workloadStatusThread.interrupt();
                try
                {
                    workloadStatusThread.join();
                }
                catch ( InterruptedException e )
                {
                    // do nothing
                }
            }
        }
    }
}