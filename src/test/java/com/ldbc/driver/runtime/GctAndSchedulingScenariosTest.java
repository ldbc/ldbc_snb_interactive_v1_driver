package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.OperationClassification.GctMode;
import com.ldbc.driver.OperationClassification.SchedulingMode;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceHelper;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.NaiveSynchronizedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.ManualTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation2;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class GctAndSchedulingScenariosTest {

    private final long ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING = 100;
    private final Duration SPINNER_SLEEP_DURATION = Duration.fromMilli(0);
    private final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    // TODO eventually a check could be added for excessive execution time
    // TODO this would need to be placed in MetricsReporter/CompletionTimeService
    // TODO or something similar that checks for initiated but uncompleted operations

    // TODO consider putting all of the following in one background thread:
    // TODO  - completion time service (possibly not, for performance reasons)
    // TODO  - metrics collection service
    // TODO  - status printout

    // TODO consider moving operation stream creation per scenario into separate methods

    @Ignore
    @Test
    public void makeTestsWithWindowedModeAfterHaveUnderstoodHowToTestItProperly() {
        assertThat(true, is(false));
    }

    // TODO do a similar test with async async, where middle READ_WRITE is blocked, and check the GCT does not progress
//    @Test
//    public void shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay()
//            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
//        /*
//            Tolerated Delay == 4
//            ASYNC                   BLOCKING
//            READ                    READ_WRITE              GCT
//            TimedNamedOperation1    TimedNamedOperation2
//        0                                                   0
//        1                                                   0
//        2   S(2)D(0)                                        0
//        3                           S(3)D(0)                3
//        4   S(4)D(3)                                        3
//        5                                                   3
//        6                           S(6)D(0) !!BLOCKS!!     3
//        7   S(7)D(3)                                        3
//        8                                                   3
//        9                           S(9)D(0) !!WAITS!!      3
//        10                                                  3
//        11  S(11)D(9) !!WAITS!!                             3
//        12                                                  3
//        13  S(13)D(3)                                       3
//        14                          !!"S(9)D(0)" DELAY!!    3
//        15                                                  3
//         */
//        List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
//                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
//                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(3), "read2"),
//                new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(3), "read3"),
//                new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(9), "read4"),
//                new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(3), "read5")
//        );
//
//        List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
//                new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
//                new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
//                new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(0), "readwrite3")
//        );
//
//        Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());
//
//        Map<Class<? extends Operation>, OperationClassification> classifications = scenario1ClassificationAsync1Blocking2();
//
//        Time workloadStartTime = Time.fromMilli(0);
//        ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);
//        int threadCount = 16;
//        // Not used when Windowed Scheduling Mode is not used
//        Duration executionWindowDuration = null;
//        Duration toleratedExecutionDelayDuration = Duration.fromMilli(4);
//
//        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
//        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(
//                TIME_SOURCE,
//                errorReporter,
//                TimeUnit.MILLISECONDS,
//                workloadStartTime);
//        ConcurrentCompletionTimeService completionTimeService = completionTimeService(errorReporter, workloadStartTime);
//
//        DummyDb db = new DummyDb();
//        Map<String, String> params = new HashMap<>();
//        params.put(DummyDb.ALLOWED_ARG, "false");
//        db.init(params);
//
//        WorkloadRunnerThread runnerThread = workloadRunnerThread(
//                TIME_SOURCE,
//                workloadStartTime,
//                operations,
//                classifications,
//                threadCount,
//                executionWindowDuration,
//                toleratedExecutionDelayDuration,
//                errorReporter,
//                metricsService,
//                completionTimeService,
//                db
//        );
//
//        runnerThread.start();
//
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(0l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(1);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(0l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(2);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(0l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
//        db.setNameAllowedValue("read1", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(1l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(3);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(1l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
//        db.setNameAllowedValue("readwrite1", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(2l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(4);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(2l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        db.setNameAllowedValue("read2", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(3l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(5);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(3l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(6);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(3l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        // DO NOT ALLOW "readwrite2" to execute
//        db.setNameAllowedValue("readwrite2", false);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(3l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(7);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(3l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        db.setNameAllowedValue("read3", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(8);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(9);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        db.setNameAllowedValue("readwrite3", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(10);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(11);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        db.setNameAllowedValue("read4", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(12);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        TIME_SOURCE.setNowFromMilli(13);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(4l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        db.setNameAllowedValue("read5", true);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(5l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(false));
//
//        // At this point maximum tolerated delay for "readwrite2" should be triggered
//        TIME_SOURCE.setNowFromMilli(14);
//        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
//        assertThat(metricsService.results().totalOperationCount(), is(5l));
//        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
//        assertThat(errorReporter.errorEncountered(), is(true));
//
//        Thread.sleep(WorkloadRunner.COMPLETION_POLLING_INTERVAL_AS_MILLI * 2);
//        assertThat(runnerThread.runnerHasCompleted(), is(true));
//    }

    @Test
    public void shouldFailWhenPreviousOperationInBlockingModePreventsNextOperationFromExecutingBeforeToleratedDelay()
            throws InterruptedException, MetricsCollectionException, DbException, CompletionTimeException, WorkloadException {
        /*
            Tolerated Delay == 4
            ASYNC                   BLOCKING
            READ                    READ_WRITE              GCT
            TimedNamedOperation1    TimedNamedOperation2
        0                                                   0
        1                                                   0
        2   S(2)D(0)                                        0
        3                           S(3)D(0)                3
        4   S(4)D(3)                                        3
        5                                                   3
        6                           S(6)D(0) !!BLOCKS!!     3
        7   S(7)D(3)                                        3
        8                                                   3
        9                           S(9)D(0) !!WAITS!!      3
        10                                                  3
        11  S(11)D(9) !!WAITS!!                             3
        12                                                  3
        13  S(13)D(3)                                       3
        14                          !!"S(9)D(0)" DELAY!!    3
        15                                                  3
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

        Map<Class<? extends Operation>, OperationClassification> classifications = scenario1ClassificationAsync1Blocking2();

        Time workloadStartTime = Time.fromMilli(0);
        ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);
        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        Duration toleratedExecutionDelayDuration = Duration.fromMilli(4);

        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                workloadStartTime);
        ConcurrentCompletionTimeService completionTimeService = completionTimeService(errorReporter, workloadStartTime);

        DummyDb db = new DummyDb();
        Map<String, String> params = new HashMap<>();
        params.put(DummyDb.ALLOWED_ARG, "false");
        db.init(params);

        WorkloadRunnerThread runnerThread = workloadRunnerThread(
                TIME_SOURCE,
                workloadStartTime,
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

        runnerThread.start();

        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(0l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(1);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(0l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(2);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(0l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        db.setNameAllowedValue("read1", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(1l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(3);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(1l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        db.setNameAllowedValue("readwrite1", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(2l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(4);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(2l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("read2", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(5);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(6);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        // DO NOT ALLOW "readwrite2" to execute
        db.setNameAllowedValue("readwrite2", false);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(7);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("read3", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(8);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(9);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("readwrite3", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(10);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(11);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("read4", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(12);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(13);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("read5", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(5l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        // At this point maximum tolerated delay for "readwrite2" should be triggered
        TIME_SOURCE.setNowFromMilli(14);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(5l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(true));

        Thread.sleep(WorkloadRunner.COMPLETION_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(runnerThread.runnerHasCompleted(), is(true));
    }

    @Test
    public void shouldSuccessfullyRunToCompletionWhenAllOperationsCompleteOnTimeAndNoDependenciesPreventOperationsFromExecuting()
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        shouldShouldRunToCompletionWhenAllOperationsCompleteOnTimeInScenario1(scenario1ClassificationAsync1Async2());
        shouldShouldRunToCompletionWhenAllOperationsCompleteOnTimeInScenario1(scenario1ClassificationAsync1Blocking2());
        shouldShouldRunToCompletionWhenAllOperationsCompleteOnTimeInScenario1(scenario1ClassificationBlocking1Async2());
        shouldShouldRunToCompletionWhenAllOperationsCompleteOnTimeInScenario1(scenario1ClassificationBlocking1Blocking2());
    }

    public void shouldShouldRunToCompletionWhenAllOperationsCompleteOnTimeInScenario1(Map<Class<? extends Operation>, OperationClassification> classifications)
            throws DriverConfigurationException, DbException, CompletionTimeException, WorkloadException, InterruptedException, MetricsCollectionException {
        /*
            READ                    READ_WRITE
            TimedNamedOperation1    TimedNamedOperation2    GCT
        0                                                   0
        1                                                   0
        2   S(2)D(0)                                        0
        3                           S(3)D(0)                3
        4   S(4)D(3)                                        3
        5                                                   3
        6                           S(6)D(0)                6
        7   S(7)D(6)                                        6
        8                                                   6
        9                           S(9)D(6)                9
        10                                                  9
        11  S(11)D(0)                                       9
        12                                                  9
        13  S(13)D(9)                                       9
         */
        List<Operation<?>> readOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation1(Time.fromMilli(2), Time.fromMilli(0), "read1"),
                new TimedNamedOperation1(Time.fromMilli(4), Time.fromMilli(3), "read2"),
                new TimedNamedOperation1(Time.fromMilli(7), Time.fromMilli(6), "read3"),
                new TimedNamedOperation1(Time.fromMilli(11), Time.fromMilli(0), "read4"),
                new TimedNamedOperation1(Time.fromMilli(13), Time.fromMilli(9), "read5")
        );

        List<Operation<?>> readWriteOperations = Lists.<Operation<?>>newArrayList(
                new TimedNamedOperation2(Time.fromMilli(3), Time.fromMilli(0), "readwrite1"),
                new TimedNamedOperation2(Time.fromMilli(6), Time.fromMilli(0), "readwrite2"),
                new TimedNamedOperation2(Time.fromMilli(9), Time.fromMilli(6), "readwrite3")
        );

        Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(readOperations.iterator(), readWriteOperations.iterator());

        Time workloadStartTime = Time.fromMilli(0);
        ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);
        int threadCount = 16;
        // Not used when Windowed Scheduling Mode is not used
        Duration executionWindowDuration = null;
        // set very high so it never triggers a failure
        Duration toleratedExecutionDelayDuration = Duration.fromMinutes(100);

        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(
                TIME_SOURCE,
                errorReporter,
                TimeUnit.MILLISECONDS,
                workloadStartTime);
        ConcurrentCompletionTimeService completionTimeService = completionTimeService(errorReporter, workloadStartTime);

        DummyDb db = new DummyDb();
        Map<String, String> params = new HashMap<>();
        params.put(DummyDb.ALLOWED_ARG, "false");
        db.init(params);

        WorkloadRunnerThread runnerThread = workloadRunnerThread(
                TIME_SOURCE,
                workloadStartTime,
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

        runnerThread.start();

        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(0l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(1);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(0l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(2);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(0l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        db.setNameAllowedValue("read1", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(1l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(3);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(1l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(0)));
        db.setNameAllowedValue("readwrite1", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(2l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(4);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(2l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("read2", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(5);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(6);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(3l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(3)));
        db.setNameAllowedValue("readwrite2", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(7);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(4l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
        db.setNameAllowedValue("read3", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(5l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(8);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(5l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(9);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(5l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(6)));
        db.setNameAllowedValue("readwrite3", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(6l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(10);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(6l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(11);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(6l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        db.setNameAllowedValue("read4", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(7l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(12);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(7l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        assertThat(errorReporter.errorEncountered(), is(false));

        TIME_SOURCE.setNowFromMilli(13);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(7l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        db.setNameAllowedValue("read5", true);
        Thread.sleep(ENOUGH_MILLISECONDS_FOR_RUNNER_THREAD_TO_DO_ITS_THING);
        assertThat(metricsService.results().totalOperationCount(), is(8l));
        assertThat(completionTimeService.globalCompletionTime(), equalTo(Time.fromMilli(9)));
        assertThat(errorReporter.errorEncountered(), is(false));

        Thread.sleep(WorkloadRunner.COMPLETION_POLLING_INTERVAL_AS_MILLI * 2);
        assertThat(runnerThread.runnerHasCompleted(), is(true));
    }

    private Map<Class<? extends Operation>, OperationClassification> scenario1ClassificationAsync1Async2() {
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));
        return classifications;
    }

    private Map<Class<? extends Operation>, OperationClassification> scenario1ClassificationAsync1Blocking2() {
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ_WRITE));
        return classifications;
    }

    private Map<Class<? extends Operation>, OperationClassification> scenario1ClassificationBlocking1Async2() {
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));
        return classifications;
    }

    private Map<Class<? extends Operation>, OperationClassification> scenario1ClassificationBlocking1Blocking2() {
        Map<Class<? extends Operation>, OperationClassification> classifications = new HashMap<>();
        classifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ));
        classifications.put(TimedNamedOperation2.class, new OperationClassification(SchedulingMode.INDIVIDUAL_ASYNC, GctMode.READ_WRITE));
        return classifications;
    }

    private ConcurrentCompletionTimeService completionTimeService(ConcurrentErrorReporter errorReporter, Time workloadStartTime)
            throws CompletionTimeException {
        Set<String> peerIds = new HashSet<>();
        return CompletionTimeServiceHelper.initializeCompletionTimeService(
                new NaiveSynchronizedConcurrentCompletionTimeService(peerIds),
                peerIds,
                errorReporter,
                workloadStartTime
        );
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
                                                      ConcurrentCompletionTimeService completionTimeService,
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
                completionTimeService,
                threadCount,
                statusDisplayInterval,
                workloadStartTime,
                toleratedExecutionDelayDuration,
                spinnerSleepDuration,
                executionWindowDuration,
                earlySpinnerOffsetDuration
        );
        WorkloadRunnerThread runnerThread = new WorkloadRunnerThread(runner);
        runnerThread.setDaemon(true);
        return runnerThread;
    }

    private class WorkloadRunnerThread extends Thread {
        private final WorkloadRunner runner;
        private final AtomicBoolean runnerHasCompleted;

        WorkloadRunnerThread(WorkloadRunner runner) {
            super(WorkloadRunnerThread.class.getName());
            this.runner = runner;
            this.runnerHasCompleted = new AtomicBoolean(false);
        }

        @Override
        public void run() {
            try {
                runner.executeWorkload();
            } catch (Throwable e) {
                e.printStackTrace();
            }
            runnerHasCompleted.set(true);
        }

        boolean runnerHasCompleted() {
            return runnerHasCompleted.get();
        }
    }
}
