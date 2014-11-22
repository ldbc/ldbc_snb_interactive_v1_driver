package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.DummyCollectingConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function0;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import org.junit.Test;
import stormpot.Poolable;
import stormpot.Slot;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadPoolOperationHandlerExecutorTest {
    TimeSource timeSource = new SystemTimeSource();

    Slot DUMMY_SLOT = new Slot() {
        @Override
        public void release(Poolable obj) {
            // do nothing
        }
    };

    class SetFlagFun implements Function0 {
        private final AtomicBoolean flag;

        SetFlagFun(AtomicBoolean flag) {
            this.flag = flag;
        }

        @Override
        public Object apply() {
            flag.set(true);
            return null;
        }
    }

    @Test
    public void executorShouldReturnExpectedResult() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException, CompletionTimeException {
        // Given
        boolean ignoreScheduledStartTime = false;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Spinner spinner = new Spinner(timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, ignoreScheduledStartTime);
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyCollectingConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();

        int threadCount = 1;
        int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount, boundedQueueSize);

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTimeAsMilli(timeSource.nowAsMilli() + 200);
        operation.setTimeStamp(timeSource.nowAsMilli() + 200);
        operation.setDependencyTimeStamp(0l);
        OperationHandler<?> handler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 42);
            }
        };
        handler.setSlot(DUMMY_SLOT);
        handler.init(timeSource, spinner, operation, dummyLocalCompletionTimeWriter, errorReporter, metricsService);
        final AtomicBoolean finished = new AtomicBoolean(false);
        handler.addOnCompleteTask(new SetFlagFun(finished));

        // When
        executor.execute(handler);

        long timeoutAsMilli = timeSource.nowAsMilli() + 1000l;
        while (timeSource.nowAsMilli() < timeoutAsMilli) {
            if (finished.get()) break;
            // wait for handler to finish
            Spinner.powerNap(100);
        }

        // Then
        assertThat(metricsService.operationResultReports().size(), is(1));
        assertThat((Integer) metricsService.operationResultReports().get(0).operationResult(), is(42));
        assertThat(metricsService.operationResultReports().get(0).resultCode(), is(1));
        executor.shutdown(1000l);
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }


    @Test
    public void executorShouldReturnAllResults() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException, CompletionTimeException {
        // Given
        boolean ignoreScheduledStartTime = false;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Spinner spinner = new Spinner(timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, ignoreScheduledStartTime);
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyCollectingConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();

        int threadCount = 1;
        int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount, boundedQueueSize);

        Operation<?> operation1 = new NothingOperation();
        operation1.setScheduledStartTimeAsMilli(timeSource.nowAsMilli() + 100l);
        operation1.setTimeStamp(operation1.scheduledStartTimeAsMilli());
        operation1.setDependencyTimeStamp(0l);
        Operation<?> operation2 = new NothingOperation();
        operation2.setScheduledStartTimeAsMilli(operation1.scheduledStartTimeAsMilli() + 100l);
        operation2.setTimeStamp(operation2.scheduledStartTimeAsMilli());
        operation2.setDependencyTimeStamp(0l);
        OperationHandler<?> handler1 = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 1);
            }
        };
        OperationHandler<?> handler2 = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 2);
            }
        };

        // When

        handler1.setSlot(DUMMY_SLOT);
        handler1.init(timeSource, spinner, operation1, dummyLocalCompletionTimeWriter, errorReporter, metricsService);
        final AtomicBoolean finished1 = new AtomicBoolean(false);
        handler1.addOnCompleteTask(new SetFlagFun(finished1));

        handler2.setSlot(DUMMY_SLOT);
        handler2.init(timeSource, spinner, operation2, dummyLocalCompletionTimeWriter, errorReporter, metricsService);
        final AtomicBoolean finished2 = new AtomicBoolean(false);
        handler2.addOnCompleteTask(new SetFlagFun(finished2));

        executor.execute(handler1);
        executor.execute(handler2);

        long timeoutAsMilli = timeSource.nowAsMilli() + 1000l;
        while (timeSource.nowAsMilli() < timeoutAsMilli) {
            if (finished1.get() && finished2.get()) break;
            // wait for handler to finish
            Spinner.powerNap(100);
        }

        // Then
        assertThat(metricsService.operationResultReports().size(), is(2));
        assertThat((Integer) metricsService.operationResultReports().get(0).operationResult(), anyOf(is(1), is(2)));
        assertThat(metricsService.operationResultReports().get(0).resultCode(), is(1));
        assertThat((Integer) metricsService.operationResultReports().get(1).operationResult(), anyOf(is(1), is(2)));
        assertThat(metricsService.operationResultReports().get(1).resultCode(), is(1));
        executor.shutdown(1000l);
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }

    @Test
    public void executorShouldThrowExceptionIfShutdownMultipleTimes() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException, CompletionTimeException {
        // Given
        boolean ignoreScheduledStartTime = false;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Spinner spinner = new Spinner(timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, ignoreScheduledStartTime);
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyCollectingConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();

        int threadCount = 1;
        int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount, boundedQueueSize);

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTimeAsMilli(timeSource.nowAsMilli() + 200l);
        operation.setTimeStamp(timeSource.nowAsMilli() + 200l);
        operation.setDependencyTimeStamp(0l);
        OperationHandler<?> handler = new OperationHandler<Operation<Integer>>() {
            @Override
            protected OperationResultReport executeOperation(Operation operation) throws DbException {
                return operation.buildResult(1, 42);
            }
        };

        // When
        handler.setSlot(DUMMY_SLOT);
        handler.init(timeSource, spinner, operation, dummyLocalCompletionTimeWriter, errorReporter, metricsService);
        final AtomicBoolean finished = new AtomicBoolean(false);
        handler.addOnCompleteTask(new SetFlagFun(finished));

        executor.execute(handler);

        long timeoutAsMilli = timeSource.nowAsMilli() + 1000l;
        while (timeSource.nowAsMilli() < timeoutAsMilli) {
            if (finished.get()) break;
            // wait for handler to finish
            Spinner.powerNap(100);
        }

        // Then
        assertThat(metricsService.operationResultReports().size(), is(1));
        assertThat((Integer) metricsService.operationResultReports().get(0).operationResult(), is(42));
        assertThat(metricsService.operationResultReports().get(0).resultCode(), is(1));
        executor.shutdown(1000l);
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        boolean exceptionThrown = false;
        try {
            executor.shutdown(1000l);
        } catch (OperationHandlerExecutorException e) {
            exceptionThrown = true;
        }

        assertThat(exceptionThrown, is(true));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }
}
