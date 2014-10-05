package com.ldbc.driver;

import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.DummyCollectingConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.NothingOperation;
import com.ldbc.driver.workloads.dummy.NothingOperationHandler;
import org.junit.Test;

public class OperationHandlerFactoryTest {
    @Test
    public void shouldRunOperationHandlerTest() throws OperationException, InterruptedException {
        Class<? extends OperationHandler> operationHandlerType = NothingOperationHandler.class;
        Operation<?> operation = new NothingOperation();
        int count = 100;
        while (count < 10000000) {
            OperationHandlerFactory reflectionOperationHandlerFactory = getReflectionOperationHandlerFactoryFor(operationHandlerType);
            OperationHandlerFactory pooledReflectionOperationHandlerFactory = getPooledReflectionOperationHandlerFactoryFor(operationHandlerType);
            Duration reflectionDuration = doOperationHandlerTest(count, reflectionOperationHandlerFactory, operation);
            Duration pooledReflectionDuration = doOperationHandlerTest(count, pooledReflectionOperationHandlerFactory, operation);
            count = count * 4;
            System.out.println(String.format("Count: %s, Reflection: %s, PooledReflection: %s", count, reflectionDuration, pooledReflectionDuration));
            reflectionOperationHandlerFactory.shutdown();
            pooledReflectionOperationHandlerFactory.shutdown();
        }
    }

    public Duration doOperationHandlerTest(int count, OperationHandlerFactory operationHandlerFactory, Operation<?> operation) throws OperationException {
        boolean ignoreScheduledStartTime = false;
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);
        LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
        ConcurrentMetricsService metricsService = new DummyCollectingConcurrentMetricsService();
        Time startTime = timeSource.now();
        for (int i = 0; i < count; i++) {
            OperationHandler<?> operationHandler = operationHandlerFactory.newOperationHandler();
            operationHandler.init(timeSource, spinner, operation, localCompletionTimeWriter, errorReporter, metricsService);
            operationHandler.cleanup();
        }
        return timeSource.now().durationGreaterThan(startTime);
    }

    OperationHandlerFactory getReflectionOperationHandlerFactoryFor(Class<? extends OperationHandler> operationHandlerType) {
        return new ReflectionOperationHandlerFactory(operationHandlerType);
    }

    OperationHandlerFactory getPooledReflectionOperationHandlerFactoryFor(Class<? extends OperationHandler> operationHandlerType) {
        return new PoolingOperationHandlerFactory(new ReflectionOperationHandlerFactory(operationHandlerType));
    }
}
