package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.OperationClassification.SchedulingMode;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.DummyCountingConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2Factory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class WorkloadRunnerComplexScenarioTests {
    private final Time WORKLOAD_START_TIME_0 = Time.fromMilli(0);
    private final long ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING = 300;
    private final Duration SPINNER_SLEEP_DURATION = Duration.fromMilli(0);
    private final ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);
    private final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
    private final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
    private final boolean recordStartTimeDelayLatency = true;

    // TODO consider putting all of the following into one, single background thread:
    // TODO  - completion time service (possibly not, for performance reasons)
    // TODO  - metrics collection service
    // TODO  - status printout

    @Ignore
    @Test
    public void writeTestsThatCheckForExcessiveExecutionTime() {
        // TODO add a mechanism to terminate if any query exceeds maximum execution time
        // TODO this would need to be placed in MetricsReporter/CompletionTimeService/Executors/?
        // TODO or something similar that checks for initiated but uncompleted operations
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void writeTestsWhereBothExecutorsHaveREADOperations() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void haveMoreVariationsOfEachTest() {
        // TODO - different completion time service implementations
        // TODO - different metrics service implementations
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void makeTestsWithWindowedModeAfterHaveUnderstoodHowToTestItProperly() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void forCleanShutdownExecutorsDefinitelyNeedToManageTheirOwnThreadPools() {
        assertThat(true, is(false));
    }

    /*
    TODO Ignore for now, for the following reasons:
    TODO - when SameThreadOperationHandlerExecutor used PreciseIndividualSynchronousStreamExecutorService blocks on OperationHandlerExecutor.execute()
    TODO - Synchronous operations are supposed to be fast operations, so fast that single threaded execution is fast enough
    TODO - when MetricsService (or similar) can test for maximum query runtime the executors will not have to do this
     */
    @Ignore
    @Test
    public void shouldCauseErrorIfLastSynchronousOperationHandlerTakesTooLongToComplete() throws CompletionTimeException, InterruptedException, MetricsCollectionException, WorkloadException, DbException {
        shouldCauseErrorIfLastSynchronousOperationHandlerTakesTooLongToComplete(4);
        shouldCauseErrorIfLastSynchronousOperationHandlerTakesTooLongToComplete(16);
    }

    public void shouldCauseErrorIfLastSynchronousOperationHandlerTakesTooLongToComplete(int threadCount) throws CompletionTimeException, InterruptedException, MetricsCollectionException, WorkloadException, DbException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedExecutionDelayDuration = Duration.fromMilli(0);
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        ConcurrentMetricsService metricsService = new DummyCountingConcurrentMetricsService();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);
        DummyDb db = new DummyDb();
        Duration durationToWaitForAllHandlersToFinishBeforeShutdown = Duration.fromMilli(10);

        try {
            /*
                Number of writers: 0 ()
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]
                Thread Pool Size: ---
                Tolerated Delay == 1
                Shutdown Timeout = 10

                ASYNC                   PROCESSING      BLOCKING                PROCESSING      ACTION           COMPLETED
                READ                                    READ_WRITE
                TimedNamedOperation1                    TimedNamedOperation2
            0                           []                                      []                               0
            1                           []                                      []                               0
            2                           []              S(2)D(0)                []              BLOCK S(2)D(0)   0
            3   S(3)D(0)                S(3)<-[]                                []                               1
            4   S(4)D(0)                S(4)<-[]                                []                               2
            5   S(5)D(0)                S(5)<-[]                                []                               3
            ...
            11                          []                                      []                               3
            12                          []                                      []                               3
            13                          []                                      []                               3      BOOM S(2) took too long
             */
            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(2), Time.fromMilli(0), "S(2)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "S(3)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "S(4)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(5), Time.fromMilli(0), "S(5)D(0)")
            );

            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.NONE));

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString(false));
            db.init(params);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    readOperations.iterator(),
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    durationToWaitForAllHandlersToFinishBeforeShutdown
            );

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // S(2)D(0) is blocked
            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            db.setNameAllowedValue("S(3)D(0)", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            db.setNameAllowedValue("S(4)D(0)", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            db.setNameAllowedValue("S(5)D(0)", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // 10 ms after last synchronous operation
            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            // TODO expect true get false
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(true));

            // free S(2)D(0) so everything shuts down cleanly
            db.setNameAllowedValue("S(2)D(0)", true);

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(false));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldCauseErrorIfAnyAsynchronousOperationHandlerTakesTooLongToComplete() throws CompletionTimeException, InterruptedException, MetricsCollectionException, WorkloadException, DbException {
        shouldCauseErrorIfAnyAsynchronousOperationHandlerTakesTooLongToComplete(4);
        shouldCauseErrorIfAnyAsynchronousOperationHandlerTakesTooLongToComplete(16);
    }

    public void shouldCauseErrorIfAnyAsynchronousOperationHandlerTakesTooLongToComplete(int threadCount) throws CompletionTimeException, InterruptedException, MetricsCollectionException, WorkloadException, DbException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        Duration toleratedExecutionDelayDuration = Duration.fromMilli(100);
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        ConcurrentMetricsService metricsService = new DummyCountingConcurrentMetricsService();
        Set<String> peerIds = new HashSet<>();
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);
        DummyDb db = new DummyDb();
        Duration durationToWaitForAllHandlersToFinishBeforeShutdown = Duration.fromMilli(10);

        try {
            /*
                Number of writers: 0 ()
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]
                Thread Pool Size: ---
                Tolerated Delay == 1
                Shutdown Timeout = 10

                ASYNC                   PROCESSING      BLOCKING                PROCESSING      ACTION           COMPLETED
                READ                                    READ_WRITE
                TimedNamedOperation1                    TimedNamedOperation2
            0                           []                                      []                               0
            1                           []                                      []                               0
            2                           []              S(2)D(0)                [S(2)]          BLOCK S(2)D(0)   0
            3   S(3)D(0)                S(3)<-[]                                [S(2)]                               1
            4   S(4)D(0)                [S(4)]                                  [S(2)]          BLOCK S(4)D(0)   1
            5   S(5)D(0)                [S(4),S(5)]                             [S(2)]          BLOCK S(5)D(0)   1
            ...
            11                          [S(4),S(5)]                             S(2)<-[]        FREE S(2)D(0)    2
            12                          [S(4),S(5)]                             []                               2
            13                          S(5)<-[S(4)]                            []              FREE S(5)D(0)    3
            14                          [S(4)]                                  []                               3
            15                          [S(4)]                                  []                               3        BOOM S(4) took too long
             */
            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(2), Time.fromMilli(0), "S(2)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "S(3)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "S(4)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(5), Time.fromMilli(0), "S(5)D(0)")
            );

            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.NONE));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(OperationClassification.SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.NONE));

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString(false));
            db.init(params);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    readOperations.iterator(),
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    durationToWaitForAllHandlersToFinishBeforeShutdown
            );

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // S(2)D(0) is blocked
            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            db.setNameAllowedValue("S(3)D(0)", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // S(4)D(0) is blocked
            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            db.setNameAllowedValue("S(2)D(0)", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            db.setNameAllowedValue("S(5)D(0)", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(14);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // 10 ms after last asynchronous operation
            TIME_SOURCE.setNowFromMilli(15);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(true));

            // free S(4)D(0) so everything shuts down cleanly
            db.setNameAllowedValue("S(4)D(0)", true);

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(false));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void oneExecutorShouldNotBeAbleToStarveAnotherOfThreads() throws WorkloadException, CompletionTimeException, DbException, InterruptedException, MetricsCollectionException {
        // fails with 1 thread, need to investigate further, probably because there is no available thread to execute an operation handler in time <-- not necessarily a bug
        // oneExecutorShouldNotBeAbleToStarveAnotherOfThreads(1);
        oneExecutorShouldNotBeAbleToStarveAnotherOfThreads(4);
        oneExecutorShouldNotBeAbleToStarveAnotherOfThreads(16);
    }

    public void oneExecutorShouldNotBeAbleToStarveAnotherOfThreads(int threadCount) throws WorkloadException, CompletionTimeException, DbException, InterruptedException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 1 (blocking)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]
                Thread Pool Size: 2
                Tolerated Delay == 3

                ASYNC                   THREADS         BLOCKING                THREADS
                READ                                    READ_WRITE                          GCT (assumes initiated time submitted quickly)  ACTION                          COMPLETED
                TimedNamedOperation1                    TimedNamedOperation2
            0                           []                                      []          1 <-- S(4)D(0) initialized                                                      0
            1                           []                                      []          1                                                                               0
            2   S(2)D(0)                [S(2)]                                  []          1                                               BLOCK S(2)D(0)                  0
            3   S(3)D(0)                [S(2),S(3)]                             []          1                                               BLOCK S(3)D(0)                  0
            4                           [S(2),S(3)]              S(4)D(0)       S(4)<-[]    4 <-- S(5)D(0) initiated                                                        1
            5                           [S(2),S(3)]              S(5)D(0)       [S(5)]      4                                               BLOCK S(5)D(0)                  1
            6                           S(3)<-[S(2)]                            [S(5)]      4                                               UNBLOCK S(3)D(0)                2
            7   S(7)D(0)                S(7)<-[S(2)]                            [S(5)]      4                                                                               3
            8                           []                                      [S(5)]      4                                                                               3
            9                           []                                      [S(5)]      4                                                                               3
            10                          []                                      [S(5)]      4                                                                               3
            11                          []                                      [S(5)]      4                                                                               3
             */
            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "S(2)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(3), Time.fromMilli(0), "S(3)D(0)"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(0), "S(7)D(0)")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(4), Time.fromMilli(0), "S(4)D(0)"),
                    new TimedNamedOperation2(Time.fromMilli(5), Time.fromMilli(0), "S(5)D(0)")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ_WRITE));

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            Duration toleratedExecutionDelayDuration = Duration.fromMilli(3);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "true");
            db.init(params);

            db.setNameAllowedValue("S(2)D(0)", false);
            db.setNameAllowedValue("S(3)D(0)", false);
            db.setNameAllowedValue("S(4)D(0)", true);
            db.setNameAllowedValue("S(5)D(0)", false);
            db.setNameAllowedValue("S(7)D(0)", true);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            // S(2)D(0) is blocked, nothing will change
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            // S(3)D(0) is blocked, nothing will change
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            // check that S(4)D(0) is able to complete (is not starved of thread)
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(4)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            // S(5)D(0) is blocked, nothing will change
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(4)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(6);
            db.setNameAllowedValue("S(3)D(0)", true);
            // S(3)D(0) is unblocked -> S(3)D(0) finishes
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(4)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            // check that S(7)D(0) is able to complete (is not starved of thread)
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(4)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(4)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // allow S(2)D(0) & S(5)D(0) to complete, so workload runner can terminate
            db.setNameAllowedValue("S(2)D(0)", true);
            db.setNameAllowedValue("S(5)D(0)", true);

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor()
            throws CompletionTimeException, InterruptedException, MetricsCollectionException, DbException, WorkloadException {
        oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor(1);
        oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor(4);
        oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor(16);
    }

    public void oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor(int threadCount)
            throws CompletionTimeException, InterruptedException, MetricsCollectionException, DbException, WorkloadException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 2 (blocking & async)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]

                Tolerated Delay == 10 <-- high enough that it plays no role in this test
                ASYNC                   BLOCKING
                READ_WRITE              READ_WRITE                  GCT (assumes initiated time submitted quickly)          ACTION
                TimedNamedOperation1    TimedNamedOperation2
            0                                                       1 <-- S(2)D(0) initialized <--S (3)D(0) initialized
            1                                                       1
            2   S(2)D(0)                                            2 <-- S(5)D(0) initialized
            3                           S(3)D(0)                    3 <-- S(4)D(0) initialized
            4                           S(4)D(0) x 10,000,000       3 <-- S(4)D(0) initialized x 9,999,999
            5   S(5)D(0)                                            3                                                       !!BLOCK S(5)D(0)!! <*>

            NOTE
             - <*> must block, otherwise S(5)D(0) may (race condition) complete before first S(4)D(0) initiated time is submitted, and GCT would advance
             - time should be advanced directly from 3 to 5
             - Async & Blocking executors will both try to submit initiated times for all due operations
             - the aim is for Async to manage to submit its 1 operation before the last Blocking time is submitted
             - the goal is to force initiated time 5 to be submitted before at least one initiated time 4 is submitted <-- illegal operation
             - this is only illegal if both executors were using the same local completion time writer, this test makes sure they are not
             */

            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(5), Time.fromMilli(0), "read2")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1")
            );

            int operationCountAtTime4 = 1000000;
            Iterator<Operation<?>> manyReadWriteOperationsAtTime4 = gf.limit(
                    new TimedNamedOperation2Factory(
                            gf.constant(Time.fromMilli(4)),
                            gf.constant(Time.fromMilli(0)),
                            gf.constant("oneOfManyReadWrite2")),
                    operationCountAtTime4);

            readWriteOperations.addAll(Lists.newArrayList(manyReadWriteOperationsAtTime4));

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ_WRITE));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ_WRITE));

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            Duration toleratedExecutionDelayDuration = Duration.fromMilli(10);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "true");
            db.init(params);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // read1 can execute
            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(2)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // readwrite1 can execute
            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // at this point read2 and all readWrite2 can execute <-- read2 must be blocked for test to do what is intended
            db.setNameAllowedValue("read2", false);
            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(3)));
            // if initiated time 4 was submitted after initiated time 5 an error should have been reported (hopefully it was not)
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // allow read2 to complete, so workload runner can terminate
            db.setNameAllowedValue("read2", true);

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldFailWhenGctWriteOperationInAsyncModePreventsGctFromAdvancingHenceBlockingAnOperationFromExecutingBeforeToleratedDelay()
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
        // fails with 1 thread, need to investigate further, probably because there is no available thread to execute an operation handler in time <-- not necessarily a bug
        // shouldFailWhenGctWriteOperationInAsyncModePreventsGctFromAdvancingHenceBlockingAnOperationFromExecutingBeforeToleratedDelay(1);
        shouldFailWhenGctWriteOperationInAsyncModePreventsGctFromAdvancingHenceBlockingAnOperationFromExecutingBeforeToleratedDelay(4);
        shouldFailWhenGctWriteOperationInAsyncModePreventsGctFromAdvancingHenceBlockingAnOperationFromExecutingBeforeToleratedDelay(16);
    }

    public void shouldFailWhenGctWriteOperationInAsyncModePreventsGctFromAdvancingHenceBlockingAnOperationFromExecutingBeforeToleratedDelay(int threadCount)
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 1 (async)
                Number of executors: 1 (async)
                Initialized to: IT[ , ] CT[0,1]

                Tolerated Delay == 3
                ASYNC                   ASYNC
                READ                    READ_WRITE              GCT (assumes initiated time submitted quickly)
                TimedNamedOperation1    TimedNamedOperation2
            0                                                   1 <~~ S(2)D(0) initialized (READ ONLY)
            1                                                   1
            2   S(2)D(0)                                        1 <-- S(3)D(0) initialized
            3                           S(3)D(0)                3 <~~ S(4)D(0) initialized (READ ONLY)
            4   S(4)D(3)                                        3 <-- S(6)D(0) initialized
            5                                                   3
            6                           S(6)D(0) !!BLOCKS!!     3 <~~ S(7)D(3) initialized (READ ONLY)
            7   S(7)D(3)                                        3 <-- S(9)D(0) initialized
            8                                                   3
            9                           S(9)D(0)                3 <~~ S(11)D(9) initialized (READ ONLY)
            10                                                  3
            11  S(11)D(9) !!WAITS!!                             3 <~~ S(13)D(3) initialized (READ ONLY)
            12                                                  3
            13  S(13)D(3)                                       3
            14                                                  3
            15  !!"S(11)D(9)" !!DELAY!!                         3
             */
            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(3), "read2"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
                    new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(9), "read4"),
                    new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(3), "read5")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                    new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                    new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(0), "readwrite3")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ_WRITE));

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            Duration toleratedExecutionDelayDuration = Duration.fromMilli(3);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "false");
            db.init(params);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            db.setNameAllowedValue("read1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            db.setNameAllowedValue("readwrite1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(6);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            // DO NOT ALLOW "readwrite2" to execute
            db.setNameAllowedValue("readwrite2", false);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(9);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("readwrite3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(10);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read4", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read5", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(14);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // At this point maximum tolerated delay for "read4" should be triggered
            TIME_SOURCE.setNowFromMilli(15);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(true));

            // allow readwrite2 to complete so the thread can be cleaned up
            db.setNameAllowedValue("readwrite2", true);

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(false));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay
            ()
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
        shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay(1);
        shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay(4);
        shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay(16);
    }

    public void shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay(int threadCount)
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 1 (blocking)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]

                Tolerated Delay == 4
                ASYNC                   BLOCKING
                READ                    READ_WRITE              GCT (assumes initiated time submitted quickly)
                TimedNamedOperation1    TimedNamedOperation2
            0                                                   1 <-- S(3)D(0) initialized <~~ S(2)D(0) initialized (READ ONLY)
            1                                                   1
            2   S(2)D(0)                                        1 <~~ S(4)D(3) initialized (READ ONLY)
            3                           S(3)D(0)                3 <-- S(6)D(0) initialized
            4   S(4)D(3)                                        3 <~~ S(7)D(3) initialized (READ ONLY)
            5                                                   3
            6                           S(6)D(0) !!BLOCKS!!     3 <-- S(9)D(0) initialized
            7   S(7)D(3)                                        3 <~~ S(11)D(9) initialized
            8                                                   3
            9                           S(9)D(0) !!WAITS!!      3
            10                                                  3
            11  S(11)D(3)                                       3 <~~ S(13)D(3) initialized
            12                                                  3
            13  S(13)D(3)                                       3
            14                          !!"S(9)D(0)" !!DELAY!!  3 [UNBLOCK S(6)D(0)]
            15
             */
            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(3), "read2"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
                    new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(3), "read4"),
                    new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(3), "read5")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                    new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                    new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(0), "readwrite3")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ_WRITE));

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            Duration toleratedExecutionDelayDuration = Duration.fromMilli(4);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "false");
            db.init(params);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            db.setNameAllowedValue("read1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            db.setNameAllowedValue("readwrite1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(6);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            // DO NOT ALLOW "readwrite2" to execute
            db.setNameAllowedValue("readwrite2", false);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(9);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("readwrite3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(10);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read4", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read5", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            // At this point maximum tolerated delay for "readwrite3" should be triggered
            TIME_SOURCE.setNowFromMilli(14);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));

            // let readwrite2 complete, so readwrite3 can start. readwrite3 then starts to execute and fails due to excessive delay
            db.setNameAllowedValue("readwrite2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(true));

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(false));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync(1);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync(4);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync(16);
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync(int threadCount)
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
        /*
            Number of writers: 1 (async)
            Number of executors: 2 (blocking & async)
            Initialized to: IT[ , ] CT[0,1]

            ASYNC                   ASYNC
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   1 <~~ S(2)D(0) initialized (READ ONLY)
        1                                                   1
        2   S(2)D(0)                                        1 <-- S(3)D(0) initialized
        3                           S(3)D(0)                3 <~~ S(4)D(0) initialized (READ ONLY)
        4   S(4)D(0)                                        3 <-- S(6)D(0) initialized
        5                                                   3 <~~ S(4)D(0) initialized (READ ONLY)
        6                           S(6)D(0)                6 <~~ S(7)D(3) initialized (READ ONLY)
        7   S(7)D(3)                                        6 <-- S(9)D(3) initialized
        8                                                   6
        9                           S(9)D(3)                9 // executor knows this is last WRITE
        10                                                  9
        11  S(11)D(0)                                       9 <~~ S(11)D(0) initialized (READ ONLY)
        12                                                  9
        13  S(13)D(6)                                       9 <~~ S(13)D(6) initialized (READ ONLY)
         */
            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ_WRITE));

            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "read2"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
                    new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(0), "read4"),
                    new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(6), "read5")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                    new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                    new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(3), "readwrite3")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            // set very high so it never triggers a failure
            Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "false");
            db.init(params);

            // TODO remove workload start time as public variable for this test class and always assume 0
            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("read1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("readwrite1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));

            TIME_SOURCE.setNowFromMilli(6);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("readwrite2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(9);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("readwrite3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(10);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            db.setNameAllowedValue("read4", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            db.setNameAllowedValue("read5", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(8l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking(1);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking(4);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking(16);
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking(int threadCount)
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 1 (blocking)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]

                ASYNC                   BLOCKING
                READ                    READ_WRITE
                TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
            0                                                   1 <-- S(3)D(0) initialized <~~ S(2)D(0) initialized (READ ONLY)
            1                                                   1
            2   S(2)D(0)                                        1 <~~ S(4)D(0) initialized (READ ONLY)
            3                           S(3)D(0)                3 <-- S(6)D(0) initialized
            4   S(4)D(0)                                        3 <~~ S(7)D(3) initialized (READ ONLY)
            5                                                   3
            6                           S(6)D(0)                6 <-- S(9)D(3) initialized
            7   S(7)D(3)                                        6 <~~ S(11)D(0) initialized (READ ONLY)
            8                                                   6
            9                           S(9)D(3)                6
            10                                                  6
            11  S(11)D(0)                                       6 <~~ S(13)D(6) initialized (READ ONLY)
            12                                                  6
            13  S(13)D(6)                                       6
             */
            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ_WRITE));

            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "read2"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
                    new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(0), "read4"),
                    new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(6), "read5")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                    new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                    new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(3), "readwrite3")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            // set very high so it never triggers a failure
            Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "false");
            db.init(params);

            // TODO remove workload start time as public variable for this test class and always assume 0
            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("read1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("readwrite1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));

            TIME_SOURCE.setNowFromMilli(6);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("readwrite2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(9);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("readwrite3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(10);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read4", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read5", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(8l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync(1);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync(4);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync(16);
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync(int threadCount)
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 1 (async)
                Number of executors: 2 (blocking & async)
                Initialized to: IT[ , ] CT[0,1]

                BLOCKING                ASYNC
                READ                    READ_WRITE
                TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
            0                                                   1 <-- S(3)D(0) initialized <~~ S(2)D(0) initialized (READ ONLY)
            1                                                   1
            2   S(2)D(0)                                        1 <~~ S(4)D(0) initialized (READ ONLY)
            3                           S(3)D(0)                3 <-- S(6)D(0) initialized
            4   S(4)D(0)                                        3 <~~ S(7)D(3) initialized (READ ONLY)
            5                                                   3
            6                           S(6)D(0)                6 <-- S(9)D(3) initialized
            7   S(7)D(3)                                        6 <~~ S(11)D(0) initialized (READ ONLY)
            8                                                   6
            9                           S(9)D(3)                9 // Executor knows this is the last WRITE
            10                                                  9
            11  S(11)D(0)                                       9 <~~ S(13)D(6) initialized (READ ONLY)
            12                                                  9
            13  S(13)D(6)                                       9
             */
            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, OperationClassification.DependencyMode.READ_WRITE));

            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "read2"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
                    new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(0), "read4"),
                    new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(6), "read5")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                    new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                    new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(3), "readwrite3")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            // set very high so it never triggers a failure
            Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "false");
            db.init(params);

            // TODO remove workload start time as public variable for this test class and always assume 0
            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("read1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("readwrite1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("read2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));

            TIME_SOURCE.setNowFromMilli(6);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("readwrite2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(9);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("readwrite3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(10);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            db.setNameAllowedValue("read4", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            db.setNameAllowedValue("read5", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(8l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking(1);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking(4);
        shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking(16);
    }

    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking(int threadCount)
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        ConcurrentMetricsService metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION,
                recordStartTimeDelayLatency);

        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService completionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);

        DummyDb db = new DummyDb();

        try {
            /*
                Number of writers: 1 (blocking)
                Number of executors: 1 (blocking)
                Initialized to: IT[ , ] CT[0,1]

                BLOCKING                BLOCKING
                READ                    READ_WRITE
                TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
            0                                                   0 <~~ S(2)D(0) initialized (READ ONLY)
            1                                                   0
            2   S(2)D(0)                                        0 <-- S(3)D(0) initialized
            3                           S(3)D(0)                1 <~~ S(4)D(0) initialized (READ ONLY)
            4   S(4)D(0)                                        3 <-- S(6)D(0) initialized
            5                                                   3
            6                           S(6)D(0)                3 <~~ S(7)D(3) initialized (READ ONLY)
            7   S(7)D(3)                                        6 <-- S(9)D(3) initialized
            8                                                   6
            9                           S(9)D(3)                6 <~~ S(11)D(0) initialized (READ ONLY)
            10                                                  6
            11  S(11)D(0)                                       6 <~~ S(13)D(6) initialized (READ ONLY)
            12                                                  6
            13  S(13)D(6)                                       6
             */
            Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
            classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ));
            classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.READ_WRITE));

            List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                    new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(0), "read2"),
                    new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
                    new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(0), "read4"),
                    new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(6), "read5")
            );

            List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                    new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                    new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                    new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(3), "readwrite3")
            );

            Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());
            // Not used when Windowed Scheduling Mode is not used
            Duration executionWindowDuration = null;
            // set very high so it never triggers a failure
            Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

            Map<String, String> params = new HashMap<>();
            params.put(DummyDb.ALLOWED_DEFAULT_ARG, "false");
            db.init(params);

            WorkloadRunnerThread runnerThread = workloadRunnerThread(
                    TIME_SOURCE,
                    WORKLOAD_START_TIME_0,
                    operations,
                    classifications,
                    threadCount,
                    executionWindowDuration,
                    toleratedExecutionDelayDuration,
                    errorReporter,
                    metricsService,
                    completionTimeService,
                    db,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
            );

            // initialize GCT
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
            completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

            TIME_SOURCE.setNowFromMilli(0);
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            runnerThread.start();

            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(1);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(2);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
            // GCT may be 0 or 1 at this stage, depending on the OperationHandlerExecutor used.
            // SameThreadOperationHandlerExecutor will be 0, as it must wait for previous operation to complete before it can initiate the next operation
            // SingleThread/ThreadPoolOperationHandlerExecutor will be 1, as it can initiate the next operation as soon as it has submitted the previous one for execution
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), anyOf(equalTo(Time.fromMilli(0)), equalTo(Time.fromMilli(1))));
            db.setNameAllowedValue("read1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(3);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(1)));
            db.setNameAllowedValue("readwrite1", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(4);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
            // GCT may be 1 or 3 at this stage, depending on the OperationHandlerExecutor used.
            // SameThreadOperationHandlerExecutor will be 1, as it must wait for previous operation to complete before it can initiate the next operation
            // SingleThread/ThreadPoolOperationHandlerExecutor will be 3, as it can initiate the next operation as soon as it has submitted the previous one for execution
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), anyOf(equalTo(Time.fromMilli(1)), equalTo(Time.fromMilli(3))));
            db.setNameAllowedValue("read2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(5);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));

            TIME_SOURCE.setNowFromMilli(6);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(3l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            db.setNameAllowedValue("readwrite2", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(7);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
            // GCT may be 3 or 6 at this stage, depending on the OperationHandlerExecutor used.
            // SameThreadOperationHandlerExecutor will be 3, as it must wait for previous operation to complete before it can initiate the next operation
            // SingleThread/ThreadPoolOperationHandlerExecutor will be 6, as it can initiate the next operation as soon as it has submitted the previous one for execution
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), anyOf(equalTo(Time.fromMilli(3)), equalTo(Time.fromMilli(6))));
            db.setNameAllowedValue("read3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(8);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(9);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("readwrite3", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(10);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(11);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(6l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read4", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(12);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            TIME_SOURCE.setNowFromMilli(13);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(7l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            db.setNameAllowedValue("read5", true);
            Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
            assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(8l));
            assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

            Duration durationToWaitForRunnerToComplete = Duration.fromMilli(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 4);
            long timeoutTimeAsMilli = TIME_SOURCE.now().plus(durationToWaitForRunnerToComplete).asMilli();
            while (TIME_SOURCE.nowAsMilli() < timeoutTimeAsMilli) {
                if (runnerThread.runnerHasCompleted()) break;
                Spinner.powerNap(100);
            }

            assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
            assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        } finally {
            db.setAllowedValueForAll(true);
            metricsService.shutdown();
            completionTimeService.shutdown();
            db.shutdown();
        }
    }

    private WorkloadRunnerThread workloadRunnerThread(TimeSource timeSource,
                                                      Time workloadStartTime,
                                                      Iterator<Operation<?>> operations,
                                                      Map<Class<? extends Operation>, OperationClassification> classifications,
                                                      int threadCount,
                                                      Duration executionWindowDuration,
                                                      Duration toleratedExecutionDelayDuration,
                                                      ConcurrentErrorReporter errorReporter,
                                                      ConcurrentMetricsService metricsService,
                                                      ConcurrentCompletionTimeService concurrentCompletionTimeService,
                                                      Db db,
                                                      Duration durationToWaitForAllHandlersToFinishBeforeShutdown)
            throws WorkloadException, CompletionTimeException, DbException {
        boolean ignoreScheduledStartTime = false;
        Duration statusDisplayInterval = Duration.fromMilli(0);
        Duration spinnerSleepDuration = SPINNER_SLEEP_DURATION;
        Duration earlySpinnerOffsetDuration = Duration.fromMilli(0);
        WorkloadRunner runner = new WorkloadRunner(
                timeSource,
                db,
                operations,
                classifications,
                metricsService,
                errorReporter,
                concurrentCompletionTimeService,
                threadCount,
                statusDisplayInterval,
                workloadStartTime,
                toleratedExecutionDelayDuration,
                spinnerSleepDuration,
                executionWindowDuration,
                earlySpinnerOffsetDuration,
                durationToWaitForAllHandlersToFinishBeforeShutdown,
                ignoreScheduledStartTime
        );
        return new WorkloadRunnerThread(runner, errorReporter);
    }

    private class WorkloadRunnerThread extends Thread {
        private final WorkloadRunner runner;
        private final AtomicBoolean runnerHasCompleted;
        private final AtomicBoolean runnerCompletedSuccessfully;
        private final ConcurrentErrorReporter errorReporter;

        WorkloadRunnerThread(WorkloadRunner runner, ConcurrentErrorReporter errorReporter) {
            super(WorkloadRunnerThread.class.getSimpleName() + "-" + System.currentTimeMillis());
            this.runner = runner;
            this.runnerHasCompleted = new AtomicBoolean(false);
            this.runnerCompletedSuccessfully = new AtomicBoolean(false);
            this.errorReporter = errorReporter;
        }

        @Override
        public void run() {
            try {
                runner.executeWorkload();
                runnerCompletedSuccessfully.set(true);
                runnerHasCompleted.set(true);
            } catch (Throwable e) {
                errorReporter.reportError(this, ConcurrentErrorReporter.stackTraceToString(e));
                runnerCompletedSuccessfully.set(false);
                runnerHasCompleted.set(true);
            }
        }

        boolean runnerHasCompleted() {
            return runnerHasCompleted.get();
        }

        boolean runnerCompletedSuccessfully() {
            return runnerCompletedSuccessfully.get();
        }
    }
}
