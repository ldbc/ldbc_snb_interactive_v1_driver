package com.ldbc.driver.stormpot_tests;

import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.OperationResultReport;
import com.ldbc.driver.stormpot_test.PoolableOperationHandlerManager;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import org.junit.Ignore;
import org.junit.Test;
import stormpot.BlazePool;
import stormpot.Config;
import stormpot.Pool;
import stormpot.Timeout;

import java.util.concurrent.TimeUnit;

public class OperationHandlerCreationTest {

    @Ignore
    @Test
    public void testWithPooling() throws InterruptedException, OperationException, DbException {
        PoolableOperationHandlerManager.PoolableOperationHandlerAllocator allocator = new PoolableOperationHandlerManager.PoolableOperationHandlerAllocator(PoolableOperationHandlerTestImpl.class);
        Config<PoolableOperationHandlerManager.PoolableOperationHandler<?>> config = new Config<PoolableOperationHandlerManager.PoolableOperationHandler<?>>().setAllocator(allocator);
        Pool<PoolableOperationHandlerManager.PoolableOperationHandler<?>> pool = new BlazePool<>(config);
        Timeout timeout = new Timeout(1, TimeUnit.SECONDS);

        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        Duration toleratedDelay = Duration.fromMilli(0);
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, executionDelayPolicy);
        TimedNamedOperation1 operation = new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "name");
        LocalCompletionTimeWriter localCompletionTimeWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(timeSource, errorReporter, TimeUnit.MILLISECONDS, Time.fromMilli(1));

        for (int i = 0; i < 1000000; i++) {
            PoolableOperationHandlerManager.PoolableOperationHandler<TimedNamedOperation1> handler = (PoolableOperationHandlerManager.PoolableOperationHandler<TimedNamedOperation1>) pool.claim(timeout);
            handler.init(
                    timeSource,
                    spinner,
                    operation,
                    localCompletionTimeWriter,
                    errorReporter,
                    metricsService
            );
            try {
                handler.executeOperationUnsafe(operation);
            } finally {
                System.out.println(handler);
                if (handler != null) {
                    handler.release();
                }
            }
        }
    }

    @Ignore
    @Test
    public void testWithoutPooling() throws InterruptedException, OperationException, DbException {
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        Duration toleratedDelay = Duration.fromMilli(0);
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, executionDelayPolicy);
        TimedNamedOperation1 operation = new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "name");
        LocalCompletionTimeWriter localCompletionTimeWriter = null;
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(timeSource, errorReporter, TimeUnit.MILLISECONDS, Time.fromMilli(1));

        for (int i = 0; i < 1000000; i++) {
            OperationHandler<TimedNamedOperation1> handler = (OperationHandler<TimedNamedOperation1>) ClassLoaderHelper.loadOperationHandler(NormalOperationHandlerTestImpl.class);
            handler.init(
                    timeSource,
                    spinner,
                    operation,
                    localCompletionTimeWriter,
                    errorReporter,
                    metricsService
            );
            try {
                handler.executeOperationUnsafe(operation);
            } finally {
            }
        }
    }

    public static class PoolableOperationHandlerTestImpl extends PoolableOperationHandlerManager.PoolableOperationHandler<TimedNamedOperation1> {
        @Override
        protected OperationResultReport executeOperation(TimedNamedOperation1 operation) throws DbException {
            // do nothing
            return null;
        }
    }

    public static class NormalOperationHandlerTestImpl extends OperationHandler<TimedNamedOperation1> {
        @Override
        protected OperationResultReport executeOperation(TimedNamedOperation1 operation) throws DbException {
            // do nothing
            return null;
        }
    }
}
