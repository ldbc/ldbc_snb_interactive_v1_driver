package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
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
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.dummy.*;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.ldbc.driver.OperationClassification.DependencyMode;
import static com.ldbc.driver.OperationClassification.SchedulingMode;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class OperationsToOperationHandlersTransformerTest {
    @Test
    public void shouldReturnExactlyOneHandlerForEveryOperationWithSmallAmountOfOperations() throws DbException, CompletionTimeException, WorkloadException {
        // Given
        TimeSource timeSource = new SystemTimeSource();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        ConcurrentCompletionTimeService completionTimeService = assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds();

        TimeUnit unit = TimeUnit.MILLISECONDS;
        Time initialTime = timeSource.now();
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(timeSource, errorReporter, unit, initialTime);

        Db db = ClassLoaderHelper.loadDb(DummyDb.class);
        Map<String, String> dbParams = new HashMap<>();
        dbParams.put(DummyDb.ALLOWED_DEFAULT_ARG, "true");
        db.init(dbParams);

        Duration toleratedDelay = Duration.fromSeconds(1);
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, executionDelayPolicy);

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, DependencyMode.READ_WRITE));
        operationClassifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, DependencyMode.READ_WRITE));
        operationClassifications.put(TimedNamedOperation3.class, new OperationClassification(SchedulingMode.WINDOWED, DependencyMode.READ_WRITE));

        OperationsToOperationHandlersTransformer operationsToOperationHandlersTransformer = new OperationsToOperationHandlersTransformer(
                timeSource,
                db,
                spinner,
                completionTimeService,
                errorReporter,
                metricsService,
                operationClassifications
        );

        List<Operation<?>> operations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(1), Time.fromMilli(0), "name0"),
                new TimedNamedOperation2(Time.fromMilli(2), Time.fromMilli(1), "name1"),
                new TimedNamedOperation3(Time.fromMilli(3), Time.fromMilli(2), "name2"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(3), "name3"),
                new TimedNamedOperation2(Time.fromMilli(5), Time.fromMilli(4), "name4"),
                new TimedNamedOperation3(Time.fromMilli(6), Time.fromMilli(5), "name5"),
                new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(6), "name6"),
                new TimedNamedOperation2(Time.fromMilli(8), Time.fromMilli(7), "name7"),
                new TimedNamedOperation3(Time.fromMilli(9), Time.fromMilli(8), "name8"),
                new TimedNamedOperation1(Time.fromMilli(10), Time.fromMilli(9), "name9")
        );

        // When
        assertThat(completionTimeService.getAllWriters().size(), is(0));
        List<OperationHandler<?>> operationHandlers = operationsToOperationHandlersTransformer.transform(operations);

        assertThat(completionTimeService.getAllWriters().size(), is(3));
        assertThat(operationHandlers.size(), is(operations.size()));
        assertThat(operationHandlers.get(0).operation().scheduledStartTime(), is(Time.fromMilli(1)));
        assertThat(operationHandlers.get(0).operation().dependencyTime(), is(Time.fromMilli(0)));
        assertThat(operationHandlers.get(1).operation().scheduledStartTime(), is(Time.fromMilli(2)));
        assertThat(operationHandlers.get(1).operation().dependencyTime(), is(Time.fromMilli(1)));
        assertThat(operationHandlers.get(2).operation().scheduledStartTime(), is(Time.fromMilli(3)));
        assertThat(operationHandlers.get(2).operation().dependencyTime(), is(Time.fromMilli(2)));
        assertThat(operationHandlers.get(3).operation().scheduledStartTime(), is(Time.fromMilli(4)));
        assertThat(operationHandlers.get(3).operation().dependencyTime(), is(Time.fromMilli(3)));
        assertThat(operationHandlers.get(4).operation().scheduledStartTime(), is(Time.fromMilli(5)));
        assertThat(operationHandlers.get(4).operation().dependencyTime(), is(Time.fromMilli(4)));
        assertThat(operationHandlers.get(5).operation().scheduledStartTime(), is(Time.fromMilli(6)));
        assertThat(operationHandlers.get(5).operation().dependencyTime(), is(Time.fromMilli(5)));
        assertThat(operationHandlers.get(6).operation().scheduledStartTime(), is(Time.fromMilli(7)));
        assertThat(operationHandlers.get(6).operation().dependencyTime(), is(Time.fromMilli(6)));
        assertThat(operationHandlers.get(7).operation().scheduledStartTime(), is(Time.fromMilli(8)));
        assertThat(operationHandlers.get(7).operation().dependencyTime(), is(Time.fromMilli(7)));
        assertThat(operationHandlers.get(8).operation().scheduledStartTime(), is(Time.fromMilli(9)));
        assertThat(operationHandlers.get(8).operation().dependencyTime(), is(Time.fromMilli(8)));
        assertThat(operationHandlers.get(9).operation().scheduledStartTime(), is(Time.fromMilli(10)));
        assertThat(operationHandlers.get(9).operation().dependencyTime(), is(Time.fromMilli(9)));
    }

    @Ignore
    @Test
    public void shouldReturnExactlyOneHandlerForEveryOperationWithLargeAmountOfOperations() throws DbException, CompletionTimeException, WorkloadException {
        /*
         SETUP
         */
        TimeSource timeSource = new SystemTimeSource();
        Time setupStartTime = timeSource.now();
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        CompletionTimeServiceAssistant assistant = new CompletionTimeServiceAssistant();
        ConcurrentCompletionTimeService completionTimeService = assistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds();

        TimeUnit unit = TimeUnit.MILLISECONDS;
        Time initialTime = timeSource.now();
        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(timeSource, errorReporter, unit, initialTime);

        Db db = ClassLoaderHelper.loadDb(DummyDb.class);
        Map<String, String> dbParams = new HashMap<>();
        dbParams.put(DummyDb.ALLOWED_DEFAULT_ARG, "true");
        db.init(dbParams);

        Duration toleratedDelay = Duration.fromSeconds(1);
        ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(timeSource, toleratedDelay, errorReporter);
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, executionDelayPolicy);

        Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
        operationClassifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, DependencyMode.READ_WRITE));
        operationClassifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, DependencyMode.READ_WRITE));
        operationClassifications.put(TimedNamedOperation3.class, new OperationClassification(SchedulingMode.WINDOWED, DependencyMode.READ_WRITE));

        OperationsToOperationHandlersTransformer operationsToOperationHandlersTransformer = new OperationsToOperationHandlersTransformer(
                timeSource,
                db,
                spinner,
                completionTimeService,
                errorReporter,
                metricsService,
                operationClassifications
        );

        Time setupFinishTime = timeSource.now();
        System.out.println("Duration to complete test setup : " + setupFinishTime.durationGreaterThan(setupStartTime));

        /*
         OPERATION GENERATION
         */
        Time generationStartTime = timeSource.now();
        long operationCountPerType = 10000000;

        List<Operation<?>> timedNamedOperation1s = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation1Factory(
                                gf.constantIncrementTime(Time.fromMilli(1), Duration.fromMilli(1)), // start times
                                gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)), // dependency times
                                gf.constant("1") //names
                        ),
                        operationCountPerType
                )
        );

        List<Operation<?>> timedNamedOperation2s = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation2Factory(
                                gf.constantIncrementTime(Time.fromMilli(1), Duration.fromMilli(1)), // start times
                                gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)), // dependency times
                                gf.constant("2") //names
                        ),
                        operationCountPerType
                )
        );

        List<Operation<?>> timedNamedOperation3s = Lists.newArrayList(
                gf.limit(
                        new TimedNamedOperation3Factory(
                                gf.constantIncrementTime(Time.fromMilli(1), Duration.fromMilli(1)), // start times
                                gf.constantIncrementTime(Time.fromMilli(0), Duration.fromMilli(1)), // dependency times
                                gf.constant("3") //names
                        ),
                        operationCountPerType
                )
        );

        Time generationFinishTime = timeSource.now();
        System.out.println("Duration to generate operations : " + generationFinishTime.durationGreaterThan(generationStartTime));

        /*
         OPERATION MERGE SORT
         */

        Time mergeSortStartTime = timeSource.now();
        List<Operation<?>> operations = Lists.newArrayList(
                gf.mergeSortOperationsByStartTime(
                        timedNamedOperation1s.iterator(),
                        timedNamedOperation2s.iterator(),
                        timedNamedOperation3s.iterator()
                )
        );
        Time mergeSortFinishTime = timeSource.now();
        System.out.println("Duration to perform merge sort: " + mergeSortFinishTime.durationGreaterThan(mergeSortStartTime));

        /*
         OPERATION TO OPERATION HANDLER TRANSFORMATION
         */

        assertThat(completionTimeService.getAllWriters().size(), is(0));
        Time transformationStartTime = timeSource.now();
        List<OperationHandler<?>> operationHandlers = operationsToOperationHandlersTransformer.transform(operations);
        Time transformationFinishTime = timeSource.now();

        assertThat(completionTimeService.getAllWriters().size(), is(3));
        assertThat(operationHandlers.size(), is(operations.size()));
        System.out.println("Duration to perform transformation: " + transformationFinishTime.durationGreaterThan(transformationStartTime));
    }

    @Ignore
    @Test
    public void optimizePerformanceOfThisClass_ItSeemsToBeVerySlowForLargerInputs() {
        assertThat(true, is(false));
    }
}
