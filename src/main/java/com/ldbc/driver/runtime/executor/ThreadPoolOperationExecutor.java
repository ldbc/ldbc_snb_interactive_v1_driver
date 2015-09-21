package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandlerRunnableContext;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.coordination.GlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.TimeSource;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ThreadPoolOperationExecutor implements OperationExecutor
{
    private final ExecutorService threadPoolExecutorService;
    private final AtomicLong uncompletedHandlers = new AtomicLong( 0 );
    private final AtomicBoolean shutdown = new AtomicBoolean( false );
    private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever;

    public ThreadPoolOperationExecutor( int threadCount,
            int boundedQueueSize,
            Db db,
            WorkloadStreams.WorkloadStreamDefinition streamDefinition,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            GlobalCompletionTimeReader globalCompletionTimeReader,
            Spinner spinner,
            TimeSource timeSource,
            ConcurrentErrorReporter errorReporter,
            MetricsService metricsService,
            ChildOperationGenerator childOperationGenerator )
    {
        this.operationHandlerRunnableContextRetriever = new OperationHandlerRunnableContextRetriever(
                streamDefinition,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                spinner,
                timeSource,
                errorReporter,
                metricsService
        );
        ThreadFactory threadFactory = new ThreadFactory()
        {
            private final long factoryTimeStampId = System.currentTimeMillis();
            int count = 0;

            @Override
            public Thread newThread( Runnable runnable )
            {
                Thread newThread = new Thread(
                        runnable,
                        ThreadPoolOperationExecutor.class.getSimpleName() + "-id(" + factoryTimeStampId + ")" +
                        "-thread(" + count++ + ")"
                );
                return newThread;
            }
        };
        this.threadPoolExecutorService = ThreadPoolExecutorWithAfterExecute.newFixedThreadPool(
                threadCount,
                threadFactory,
                uncompletedHandlers,
                boundedQueueSize,
                childOperationGenerator,
                operationHandlerRunnableContextRetriever,
                errorReporter
        );
    }

    @Override
    public final void execute( Operation operation ) throws OperationExecutorException
    {
        uncompletedHandlers.incrementAndGet();
        try
        {
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    operationHandlerRunnableContextRetriever.getInitializedHandlerFor( operation );
            threadPoolExecutorService.execute( operationHandlerRunnableContext );
        }
        catch ( Throwable e )
        {
            throw new OperationExecutorException(
                    format( "Error retrieving handler\nOperation: %s\n%s",
                            operation,
                            ConcurrentErrorReporter.stackTraceToString( e ) ),
                    e
            );
        }
    }

    @Override
    synchronized public final void shutdown( long waitAsMilli ) throws OperationExecutorException
    {
        if ( shutdown.get() )
        {
            throw new OperationExecutorException( "Executor has already been shutdown" );
        }
        try
        {
            threadPoolExecutorService.shutdown();
            boolean allHandlersCompleted =
                    threadPoolExecutorService.awaitTermination( waitAsMilli, TimeUnit.MILLISECONDS );
            if ( false == allHandlersCompleted )
            {
                List<Runnable> stillRunningThreads = threadPoolExecutorService.shutdownNow();
                if ( false == stillRunningThreads.isEmpty() )
                {
                    String errMsg = format(
                            "%s shutdown before all handlers could complete\n%s handlers were queued for execution " +
                            "but not yet started\n%s handlers were mid-execution",
                            getClass().getSimpleName(),
                            stillRunningThreads.size(),
                            uncompletedHandlers.get() - stillRunningThreads.size() );
                    throw new OperationExecutorException( errMsg );
                }
            }
        }
        catch ( Throwable e )
        {
            throw new OperationExecutorException( "Error encountered while trying to shutdown", e );
        }
        finally
        {
            shutdown.set( true );
        }
    }

    @Override
    public long uncompletedOperationHandlerCount()
    {
        return uncompletedHandlers.get();
    }

    private static class ThreadPoolExecutorWithAfterExecute extends ThreadPoolExecutor
    {
        private final ChildOperationGenerator childOperationGenerator;
        private final ChildOperationExecutor childOperationExecutor;
        private final OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever;
        private final ConcurrentErrorReporter errorReporter;

        public static ThreadPoolExecutorWithAfterExecute newFixedThreadPool( int threadCount,
                ThreadFactory threadFactory,
                AtomicLong uncompletedHandlers,
                int boundedQueueSize,
                ChildOperationGenerator childOperationGenerator,
                OperationHandlerRunnableContextRetriever operationHandlerRunnableContextInitializer,
                ConcurrentErrorReporter errorReporter )
        {
            int corePoolSize = threadCount;
            int maximumPoolSize = threadCount;
            long keepAliveTime = 0;
            TimeUnit unit = TimeUnit.MILLISECONDS;
            BlockingQueue<Runnable> workQueue = DefaultQueues.newAlwaysBlockingBounded( boundedQueueSize );
            return new ThreadPoolExecutorWithAfterExecute(
                    corePoolSize,
                    maximumPoolSize,
                    keepAliveTime,
                    unit,
                    workQueue,
                    threadFactory,
                    uncompletedHandlers,
                    childOperationGenerator,
                    operationHandlerRunnableContextInitializer,
                    errorReporter
            );
        }

        private final AtomicLong uncompletedHandlers;

        private ThreadPoolExecutorWithAfterExecute( int corePoolSize,
                int maximumPoolSize,
                long keepAliveTime,
                TimeUnit unit,
                BlockingQueue<Runnable> workQueue,
                ThreadFactory threadFactory,
                AtomicLong uncompletedHandlers,
                ChildOperationGenerator childOperationGenerator,
                OperationHandlerRunnableContextRetriever operationHandlerRunnableContextRetriever,
                ConcurrentErrorReporter errorReporter )
        {
            super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory );
            this.childOperationExecutor = new ChildOperationExecutor();
            this.uncompletedHandlers = uncompletedHandlers;
            this.childOperationGenerator = childOperationGenerator;
            this.operationHandlerRunnableContextRetriever = operationHandlerRunnableContextRetriever;
            this.errorReporter = errorReporter;
        }

        // Note, this occurs in same worker thread as beforeExecute() and run()
        @Override
        protected void afterExecute( Runnable runnable, Throwable throwable )
        {
            super.afterExecute( runnable, throwable );
            OperationHandlerRunnableContext operationHandlerRunnableContext =
                    (OperationHandlerRunnableContext) runnable;
            try
            {
                childOperationExecutor.execute(
                        childOperationGenerator,
                        operationHandlerRunnableContext.operation(),
                        operationHandlerRunnableContext.resultReporter().result(),
                        operationHandlerRunnableContext.resultReporter().actualStartTimeAsMilli(),
                        operationHandlerRunnableContext.resultReporter().runDurationAsNano(),
                        operationHandlerRunnableContextRetriever
                );
            }
            catch ( Throwable e )
            {
                errorReporter.reportError( this,
                        format( "Error retrieving handler\n%s", ConcurrentErrorReporter.stackTraceToString( e ) ) );
            }
            finally
            {
                uncompletedHandlers.decrementAndGet();
                operationHandlerRunnableContext.cleanup();
            }
        }
    }
}
