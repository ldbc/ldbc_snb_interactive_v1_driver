package com.ldbc.driver.runtime.executor;

import com.ldbc.driver.*;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.DummyCollectingConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.Function0;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import org.junit.Ignore;
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

    ;

    @Ignore
    @Test
    public void addGctWriteOnlyMode() {
        // TODO NONE, READ, WRITE (add this), READ_WRITE
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void inWindowedModeOperationsShouldNotPerformGctCheckThemselvesInsteadTheExecutorShouldDoSoBeforeWindowExecution() {
        // TODO synchronize code and documentation on the way we do such things
        assertThat(true, is(false));
    }

    @Test
    public void executorShouldReturnExpectedResult() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException, CompletionTimeException {
        // Given
        boolean ignoreScheduledStartTime = false;
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy, Duration.fromMilli(0), ignoreScheduledStartTime);
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyCollectingConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(timeSource.now().plus(Duration.fromMilli(200)));
        operation.setDependencyTime(Time.fromMilli(0));
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

        Time timeout = timeSource.now().plus(Duration.fromMilli(1000));
        while (timeSource.now().lt(timeout)) {
            if (finished.get()) break;
            // wait for handler to finish
            Spinner.powerNap(100);
        }

        // Then
        assertThat(metricsService.operationResultReports().size(), is(1));
        assertThat((Integer) metricsService.operationResultReports().get(0).operationResult(), is(42));
        assertThat(metricsService.operationResultReports().get(0).resultCode(), is(1));
        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }


    @Test
    public void executorShouldReturnAllResults() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException, CompletionTimeException {
        // Given
        boolean ignoreScheduledStartTime = false;
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy, Duration.fromMilli(0), ignoreScheduledStartTime);
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyCollectingConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation1 = new NothingOperation();
        operation1.setScheduledStartTime(timeSource.now().plus(Duration.fromMilli(100)));
        operation1.setDependencyTime(Time.fromMilli(0));
        Operation<?> operation2 = new NothingOperation();
        operation2.setScheduledStartTime(operation1.scheduledStartTime().plus(Duration.fromMilli(100)));
        operation2.setDependencyTime(Time.fromMilli(0));
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

        Time timeout = timeSource.now().plus(Duration.fromMilli(1000));
        while (timeSource.now().lt(timeout)) {
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
        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }

    @Test
    public void executorShouldThrowExceptionIfShutdownMultipleTimes() throws OperationHandlerExecutorException, ExecutionException, InterruptedException, OperationException, CompletionTimeException {
        // Given
        boolean ignoreScheduledStartTime = false;
        Duration toleratedDelay = Duration.fromMilli(100);
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ExecutionDelayPolicy delayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Spinner spinner = new Spinner(timeSource, Spinner.DEFAULT_SLEEP_DURATION_10_MILLI, delayPolicy, Duration.fromMilli(0), ignoreScheduledStartTime);
        LocalCompletionTimeWriter dummyLocalCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        DummyCollectingConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();

        int threadCount = 1;
        OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(threadCount);

        Operation<?> operation = new NothingOperation();
        operation.setScheduledStartTime(timeSource.now().plus(Duration.fromMilli(200)));
        operation.setDependencyTime(Time.fromMilli(0));
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

        Time timeout = timeSource.now().plus(Duration.fromMilli(1000));
        while (timeSource.now().lt(timeout)) {
            if (finished.get()) break;
            // wait for handler to finish
            Spinner.powerNap(100);
        }

        // Then
        assertThat(metricsService.operationResultReports().size(), is(1));
        assertThat((Integer) metricsService.operationResultReports().get(0).operationResult(), is(42));
        assertThat(metricsService.operationResultReports().get(0).resultCode(), is(1));
        executor.shutdown(Duration.fromSeconds(1));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        boolean exceptionThrown = false;
        try {
            executor.shutdown(Duration.fromSeconds(1));
        } catch (OperationHandlerExecutorException e) {
            exceptionThrown = true;
        }

        assertThat(exceptionThrown, is(true));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
    }
}
