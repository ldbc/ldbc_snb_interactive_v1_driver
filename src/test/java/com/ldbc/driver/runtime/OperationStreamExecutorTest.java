package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.OperationClassification.GctMode;
import com.ldbc.driver.OperationClassification.SchedulingMode;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNameOperation2Factory;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class OperationStreamExecutorTest {
    private final Time WORKLOAD_START_TIME_0 = Time.fromMilli(0);
    private final long ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING = 200;
    private final Duration SPINNER_SLEEP_DURATION = Duration.fromMilli(0);
    private final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
    private final CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
    private final ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);

    private ConcurrentErrorReporter errorReporter;
    private ConcurrentMetricsService metricsService;
    private ConcurrentCompletionTimeService completionTimeService;

    @Before
    public void initialise() throws CompletionTimeException {
        errorReporter = new ConcurrentErrorReporter();
        metricsService = new ThreadedQueuedConcurrentMetricsService(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                WORKLOAD_START_TIME_0);
        completionTimeService = completionTimeService(TIME_SOURCE, errorReporter);
    }

    @After
    public void cleanup() throws MetricsCollectionException, CompletionTimeException {
        metricsService.shutdown();
        completionTimeService.shutdown();
    }

    // TODO use try catch finally instead of "initialise()" & "cleanup()"
    // TODO that will also allow for having 2 versions of each tests, for different completion time service implementations

    // TODO test where both executors have READ_WRITE operations

    // TODO test where both executors have READ operations only

    // TODO test with different thread counts

    // TODO eventually a check could be added for excessive execution time
    // TODO this would need to be placed in MetricsReporter/CompletionTimeService
    // TODO or something similar that checks for initiated but uncompleted operations

    // TODO consider putting all of the following into one, single background thread:
    // TODO  - completion time service (possibly not, for performance reasons)
    // TODO  - metrics collection service
    // TODO  - status printout

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

    @Ignore
    @Test
    public void addTestToExposeTheNeedForRevisedThreadPoolManagement_ToFixStarvationProblem() {
        // TODO have 2 executors, Async/Read & Blocking/ReadWrite
        // TODO have high (sufficient) tolerated delay
        // TODO set thread pool size to 2
        // TODO in Async/Read start 2 operations, and block them
        // TODO in Blocking start 1 operation (I assume it will block, waiting for a thread to be available)
        // TODO assert that ReadWrite thread is blocked (GCT does not advance)
        // TODO in Blocking start 1 more operation (it will block, waiting first for prev operation and then for a thread to be available) - block that thread manually
        // TODO unblock one Async/Read operation and observe that the following changes: metrics.op_count, gct (because of Blocking/ReadWrite op)
        // TODO if possible, check GCT.LCT.initiatedTime to assert that second Blocking/ReadWrite operation has started
        // TODO in Async/Read try to start 1 more operation (I assume it will block, waiting for a thread to be available - 1 Async/Read & 1 Blocking/ReadWrite are already running)
        // TODO assert that Async/Read thread is blocked (metrics.op_count) - waiting for thread to be available
        // TODO wait until tolerated delay for this new Async/Read operation, then assert error is triggered
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void oneExecutorShouldNotBeCapableOfAdvancingInitiatedTimeOfAnotherExecutor()
            throws CompletionTimeException, InterruptedException, MetricsCollectionException, DbException, WorkloadException {
        // TODO remove
        Thread.sleep(Duration.fromSeconds(10).asMilli());
        /*
            Number of writers: 1 (blocking & async)
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
        // TODO remove
        System.out.println(1);

        List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                new TimedNamedOperation1(Time.fromMilli(5), Time.fromMilli(0), "read2")
        );
        // TODO remove
        System.out.println(2);

        List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1")
        );
        // TODO remove
        System.out.println(3);
        Iterator<Operation<?>> manyReadWriteOperationsAtTime4 = gf.limit(
                new TimedNameOperation2Factory(
                        gf.constant(Time.fromMilli(4)),
                        gf.constant(Time.fromMilli(0)),
                        gf.constant("oneOfManyReadWrite2")),
                10000000);
        // TODO remove
        System.out.println(4);
        readWriteOperations.addAll(Lists.newArrayList(manyReadWriteOperationsAtTime4));

        // TODO remove
        System.out.println(5);
        Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

        // TODO remove
        System.out.println(6);
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ_WRITE));

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        Duration toleratedExecutionDelayDuration = Duration.fromMilli(10);

        // TODO remove
        System.out.println(7);
        DummyDb db = new DummyDb();
        Map<String, String> params = new HashMap<>();
        params.put(DummyDb.ALLOWED_DEFAULT_ARG, "true");
        db.init(params);

        // TODO remove
        System.out.println(8);
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
                db
        );

        // TODO remove
        System.out.println(9);

        // initialize GCT
        completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(0));
        completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, Time.fromMilli(1));

        // TODO remove
        System.out.println(10);
        TIME_SOURCE.setNowFromMilli(0);
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(0)));
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // TODO remove
        System.out.println(11);
        runnerThread.start();

        // TODO remove
        System.out.println(12);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // TODO remove
        System.out.println(13);
        TIME_SOURCE.setNowFromMilli(1);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
        // TODO remove
        System.out.println(14);

        // read1 can execute
        TIME_SOURCE.setNowFromMilli(2);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(1l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(2)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // TODO remove
        System.out.println(15);
        // readwrite1 can execute
        TIME_SOURCE.setNowFromMilli(3);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(2l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // TODO remove
        System.out.println(16);
        // at this point read2 and all readWrite2 can execute <-- read2 must be blocked for test to do what is intended
        db.setNameAllowedValue("read2", false);
        TIME_SOURCE.setNowFromMilli(5);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), is(Time.fromMilli(3)));
        // if initiated time 4 was submitted after initiated time 5 an error should have been reported (hopefully it was not)
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // TODO remove
        System.out.println(17);
        // allow read2 to complete, so workload runner can terminate
        db.setNameAllowedValue("read2", true);

        // TODO remove
        System.out.println(18);
        System.out.println(metricsService.results().totalOperationCount());

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        // TODO remove
        System.out.println(19);
        for (int i = 0; i < 100; i++) {
            System.out.println(metricsService.results().totalOperationCount());
            Spinner.powerNap(100);
        }

        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
    }

    @Test
    public void shouldFailWhenGctWriteOperationInAsyncModePreventsGctFromAdvancingHenceBlockingAnOperationFromExecutingBeforeToleratedDelay()
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
        /*
            Number of writers: 1 (blocking)
            Number of executors: 2 (blocking & async)
            Initialized to: IT[ , ] CT[0,1]

            Tolerated Delay == 3
            ASYNC                   ASYNC
            READ                    READ_WRITE              GCT (assumes initiated time submitted quickly)
            TimedNamedOperation1    TimedNamedOperation2
        0                                                   0 <~~ S(2)D(0) initialized (READ ONLY)
        1                                                   0
        2   S(2)D(0)                                        1 <-- S(3)D(0) initialized
        3                           S(3)D(0)                1 <~~ S(4)D(0) initialized (READ ONLY)
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
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        Duration toleratedExecutionDelayDuration = Duration.fromMilli(3);

        DummyDb db = new DummyDb();
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
                db
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
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(1);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(0l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
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
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
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

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(false));
    }

    @Test
    public void shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay()
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
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
        11  S(11)D(9) !!WAITS!!                             3 <~~ S(13)D(3) initialized
        12                                                  3
        13  S(13)D(3)                                       3
        14                          !!"S(9)D(0)" !!DELAY!!  3
        15
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
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ_WRITE));

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        Duration toleratedExecutionDelayDuration = Duration.fromMilli(4);

        DummyDb db = new DummyDb();
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
                db
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
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(12);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(13);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(4l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("read5", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // At this point maximum tolerated delay for "readwrite2" should be triggered
        TIME_SOURCE.setNowFromMilli(14);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(errorReporter.toString(), metricsService.results().totalOperationCount(), is(5l));
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(true));

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(false));
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteAsync()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        /*
            Number of writers: 1 (async)
            Number of executors: 2 (blocking & async)
            Initialized to: IT[ , ] CT[0,1]

            ASYNC                   ASYNC
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   0 <~~ S(2)D(0) initialized (READ ONLY)
        1                                                   0
        2   S(2)D(0)                                        1 <-- S(3)D(0) initialized
        3                           S(3)D(0)                1 <~~ S(4)D(0) initialized (READ ONLY)
        4   S(4)D(0)                                        3 <-- S(6)D(0) initialized
        5                                                   3 <~~ S(4)D(0) initialized (READ ONLY)
        6                           S(6)D(0)                3 <~~ S(7)D(3) initialized (READ ONLY)
        7   S(7)D(3)                                        6 <-- S(9)D(3) initialized
        8                                                   6
        9                           S(9)D(3)                6
        10                                                  6
        11  S(11)D(0)                                       6 <~~ S(11)D(0) initialized (READ ONLY)
        12                                                  6
        13  S(13)D(6)                                       6 <~~ S(13)D(6) initialized (READ ONLY)
         */
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));

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

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        // set very high so it never triggers a failure
        Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

        DummyDb db = new DummyDb();
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
                db
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
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
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
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
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

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadAsyncReadWriteBlocking()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
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
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ_WRITE));

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

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        // set very high so it never triggers a failure
        Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

        DummyDb db = new DummyDb();
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
                db
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

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteAsync()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
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
        9                           S(9)D(3)                6
        10                                                  6
        11  S(11)D(0)                                       6 <~~ S(13)D(6) initialized (READ ONLY)
        12                                                  6
        13  S(13)D(6)                                       6
         */
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));

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

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        // set very high so it never triggers a failure
        Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

        DummyDb db = new DummyDb();
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
                db
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

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
    }

    @Test
    public void shouldSuccessfullyCompleteWhenAllOperationsFinishOnTimeWithReadBlockingReadWriteBlocking()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        /*
            Number of writers: 1 (blocking)
            Number of executors: 1 (blocking)
            Initialized to: IT[ , ] CT[0,1]

            BLOCKING                BLOCKING
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT (assumes initiated time submitted quickly)
        0                                                   0 <~~ S(2)D(0) initialized (READ ONLY)
        1                                                   0
        2   S(2)D(0)                                        1 <-- S(3)D(0) initialized
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
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ_WRITE));

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

        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        // set very high so it never triggers a failure
        Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

        DummyDb db = new DummyDb();
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
                db
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
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(1)));
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
        assertThat(errorReporter.toString(), completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
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

        Thread.sleep(WorkloadRunner.RUNNER_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(errorReporter.toString(), runnerThread.runnerHasCompleted(), is(true));
        assertThat(errorReporter.toString(), runnerThread.runnerCompletedSuccessfully(), is(true));
    }

    private ConcurrentCompletionTimeService completionTimeService(TimeSource timeSource, ConcurrentErrorReporter errorReporter)
            throws CompletionTimeException {
        Set<String> peerIds = new HashSet<>();
        // TODO test also with threaded completion time service implementation
        ConcurrentCompletionTimeService concurrentCompletionTimeService =
                completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(peerIds);
//        ConcurrentCompletionTimeService concurrentCompletionTimeService =
//                completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(timeSource,peerIds,errorReporter);
        return concurrentCompletionTimeService;
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
                                                      Db db)
            throws WorkloadException, CompletionTimeException, DbException {
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
                earlySpinnerOffsetDuration
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
                runnerCompletedSuccessfully.set(false);
                runnerHasCompleted.set(true);
                errorReporter.reportError(this, ConcurrentErrorReporter.stackTraceToString(e));
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
