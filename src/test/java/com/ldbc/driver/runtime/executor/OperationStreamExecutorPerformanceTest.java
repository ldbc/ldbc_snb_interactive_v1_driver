package com.ldbc.driver.runtime.executor;

import com.google.common.collect.Lists;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.DummyGlobalCompletionTimeReader;
import com.ldbc.driver.runtime.coordination.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.DummyCountingConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.*;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class OperationStreamExecutorPerformanceTest {
    private final ManualTimeSource timeSource = new ManualTimeSource(0);
    private final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    /*
2014/09/??

    Spinner [Sleep = 0 ms] (OLD thread OLD executor) 100000 ops in 00:00.063 (m:s.ms): 1575.299306868305 ops/ms
    Spinner [Sleep = 0 ms] (OLD thread NEW executor) 100000 ops in 00:00.058 (m:s.ms): 1696.06512890095 ops/ms
    Spinner [Sleep = 0 ms] (NEW thread OLD executor) 100000 ops in 00:00.055 (m:s.ms): 1800.1800180018001 ops/ms
    Spinner [Sleep = 0 ms] (NEW thread NEW executor) 100000 ops in 00:00.049 (m:s.ms): 2012.477359629704 ops/ms

    Spinner [Sleep = 1 ms] (OLD thread OLD executor) 10000 ops in 00:04.506 (m:s.ms): 2.218859417505026 ops/ms
    Spinner [Sleep = 1 ms] (OLD thread NEW executor) 10000 ops in 00:00.005 (m:s.ms): 1769.9115044247787 ops/ms
    Spinner [Sleep = 1 ms] (NEW thread OLD executor) 10000 ops in 00:00.005 (m:s.ms): 1824.817518248175 ops/ms
    Spinner [Sleep = 1 ms] (NEW thread NEW executor) 10000 ops in 00:00.004 (m:s.ms): 2032.5203252032522 ops/ms

    Spinner [Sleep = 10 ms] (OLD thread OLD executor) 1000 ops in 00:04.420 (m:s.ms): 0.2262034020991676 ops/ms
    Spinner [Sleep = 10 ms] (OLD thread NEW executor) 1000 ops in 00:00.000 (m:s.ms): 1923.076923076923 ops/ms
    Spinner [Sleep = 10 ms] (NEW thread OLD executor) 1000 ops in 00:00.000 (m:s.ms): 1369.86301369863 ops/ms
    Spinner [Sleep = 10 ms] (NEW thread NEW executor) 1000 ops in 00:00.000 (m:s.ms): 2127.6595744680853 ops/ms

2014/09/26

    Spinner [Sleep = 0 ms] (thread pool executor) 100000 ops in 00:00.057 (m:s.ms): 1731.6017316017317 ops/ms
    Spinner [Sleep = 0 ms] (single thread executor) 100000 ops in 00:00.050 (m:s.ms): 1980.1980198019803 ops/ms
    Spinner [Sleep = 0 ms] (same thread executor) 100000 ops in 00:00.016 (m:s.ms): 6191.9504643962855 ops/ms

    Spinner [Sleep = 1 ms] (thread pool executor) 100000 ops in 00:00.051 (m:s.ms): 1942.8793471925392 ops/ms
    Spinner [Sleep = 1 ms] (single thread executor) 100000 ops in 00:00.054 (m:s.ms): 1843.9977872026552 ops/ms
    Spinner [Sleep = 1 ms] (same thread executor) 100000 ops in 00:00.015 (m:s.ms): 6317.119393556539 ops/ms

    Spinner [Sleep = 10 ms] (thread pool executor) 100000 ops in 00:00.053 (m:s.ms): 1876.172607879925 ops/ms
    Spinner [Sleep = 10 ms] (single thread executor) 100000 ops in 00:00.047 (m:s.ms): 2100.8403361344535 ops/ms
    Spinner [Sleep = 10 ms] (same thread executor) 100000 ops in 00:00.015 (m:s.ms): 6377.551020408164 ops/ms
     */

    @Test
    public void synchronousExecutorPerformanceTest() throws CompletionTimeException, MetricsCollectionException, DbException, OperationHandlerExecutorException {
        int experimentRepetitions;
        long operationCount;
        Duration spinnerSleepDuration;

        experimentRepetitions = 100;
        operationCount = 100000;
        spinnerSleepDuration = Duration.fromMilli(0);
        synchronousExecutorPerformanceTestWithSpinnerDuration(spinnerSleepDuration, experimentRepetitions, operationCount);

        experimentRepetitions = 100;
        operationCount = 100000;
        spinnerSleepDuration = Duration.fromMilli(1);
        synchronousExecutorPerformanceTestWithSpinnerDuration(spinnerSleepDuration, experimentRepetitions, operationCount);

        experimentRepetitions = 100;
        operationCount = 100000;
        spinnerSleepDuration = Duration.fromMilli(10);
        synchronousExecutorPerformanceTestWithSpinnerDuration(spinnerSleepDuration, experimentRepetitions, operationCount);
    }

    public void synchronousExecutorPerformanceTestWithSpinnerDuration(Duration spinnerSleepDuration, int experimentRepetitions, long operationCount)
            throws CompletionTimeException, MetricsCollectionException, DbException, OperationHandlerExecutorException {
        List<Duration> threadPoolExecutorTimes = new ArrayList<>();
        List<Duration> singleThreadExecutorTimes = new ArrayList<>();
        List<Duration> sameThreadExecutorTimes = new ArrayList<>();

        List<Operation<?>> operations = Lists.newArrayList(getOperations(operationCount));

        while (experimentRepetitions-- > 0) {
            // Thread Pool Executor
            {
                boolean ignoreScheduledStartTime = false;
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);
                DummyDb db = new DummyDb();
                Map<String, String> dummyDbParameters = new HashMap<>();
                dummyDbParameters.put(DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString(true));
                db.init(dummyDbParameters);
                LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
                ConcurrentMetricsService metricsService = new DummyCountingConcurrentMetricsService();
                DummyGlobalCompletionTimeReader globalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
                globalCompletionTimeReader.setGlobalCompletionTime(Time.fromNano(0));
                AtomicBoolean executorHasFinished = new AtomicBoolean(false);
                AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);
                timeSource.setNowFromMilli(0);

                WorkloadStreams.WorkloadStreamDefinition streamDefinition = new WorkloadStreams.WorkloadStreamDefinition(
                        new HashSet<Class<? extends Operation<?>>>(),
                        Collections.<Operation<?>>emptyIterator(),
                        operations.iterator()
                );

                OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(1, DefaultQueues.DEFAULT_BOUND_1000);
                PreciseIndividualBlockingOperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        streamDefinition,
                        spinner,
                        executor,
                        db,
                        localCompletionTimeWriter,
                        metricsService,
                        globalCompletionTimeReader,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                threadPoolExecutorTimes.add(doTest(thread, errorReporter, metricsService, operationCount));
                executor.shutdown(Duration.fromSeconds(1));
                db.shutdown();
                metricsService.shutdown();
            }
            // Single Thread Executor
            {
                boolean ignoreScheduledStartTime = false;
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);
                DummyDb db = new DummyDb();
                Map<String, String> dummyDbParameters = new HashMap<>();
                dummyDbParameters.put(DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString(true));
                db.init(dummyDbParameters);
                LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
                ConcurrentMetricsService metricsService = new DummyCountingConcurrentMetricsService();
                DummyGlobalCompletionTimeReader globalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
                globalCompletionTimeReader.setGlobalCompletionTime(Time.fromNano(0));
                AtomicBoolean executorHasFinished = new AtomicBoolean(false);
                AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);
                timeSource.setNowFromMilli(0);

                WorkloadStreams.WorkloadStreamDefinition streamDefinition = new WorkloadStreams.WorkloadStreamDefinition(
                        new HashSet<Class<? extends Operation<?>>>(),
                        Collections.<Operation<?>>emptyIterator(),
                        operations.iterator()
                );

                OperationHandlerExecutor executor = new SingleThreadOperationHandlerExecutor(errorReporter, DefaultQueues.DEFAULT_BOUND_1000);
                PreciseIndividualBlockingOperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        streamDefinition,
                        spinner,
                        executor,
                        db,
                        localCompletionTimeWriter,
                        metricsService,
                        globalCompletionTimeReader,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                singleThreadExecutorTimes.add(doTest(thread, errorReporter, metricsService, operationCount));
                executor.shutdown(Duration.fromSeconds(1));
                db.shutdown();
                metricsService.shutdown();
            }
            // Same Thread Executor
            {
                boolean ignoreScheduledStartTime = false;
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                Spinner spinner = new Spinner(timeSource, spinnerSleepDuration, ignoreScheduledStartTime);
                DummyDb db = new DummyDb();
                Map<String, String> dummyDbParameters = new HashMap<>();
                dummyDbParameters.put(DummyDb.ALLOWED_DEFAULT_ARG, Boolean.toString(true));
                db.init(dummyDbParameters);
                LocalCompletionTimeWriter localCompletionTimeWriter = new DummyLocalCompletionTimeWriter();
                ConcurrentMetricsService metricsService = new DummyCountingConcurrentMetricsService();
                DummyGlobalCompletionTimeReader globalCompletionTimeReader = new DummyGlobalCompletionTimeReader();
                globalCompletionTimeReader.setGlobalCompletionTime(Time.fromNano(0));
                AtomicBoolean executorHasFinished = new AtomicBoolean(false);
                AtomicBoolean forceThreadToTerminate = new AtomicBoolean(false);
                timeSource.setNowFromMilli(0);

                WorkloadStreams.WorkloadStreamDefinition streamDefinition = new WorkloadStreams.WorkloadStreamDefinition(
                        new HashSet<Class<? extends Operation<?>>>(),
                        Collections.<Operation<?>>emptyIterator(),
                        operations.iterator()
                );

                OperationHandlerExecutor executor = new SameThreadOperationHandlerExecutor();
                PreciseIndividualBlockingOperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        streamDefinition,
                        spinner,
                        executor,
                        db,
                        localCompletionTimeWriter,
                        metricsService,
                        globalCompletionTimeReader,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                sameThreadExecutorTimes.add(doTest(thread, errorReporter, metricsService, operationCount));
                executor.shutdown(Duration.fromSeconds(1));
                db.shutdown();
                metricsService.shutdown();
            }
        }

        Duration meanThreadPool = meanDuration(threadPoolExecutorTimes);
        System.out.println(String.format("Spinner [Sleep = %s ms] (thread pool executor) %s ops in %s: %s ops/ms", spinnerSleepDuration.asMilli(), operationCount, meanThreadPool, (operationCount / (double) meanThreadPool.asNano()) * 1000000));
        Duration meanSingleThread = meanDuration(singleThreadExecutorTimes);
        System.out.println(String.format("Spinner [Sleep = %s ms] (single thread executor) %s ops in %s: %s ops/ms", spinnerSleepDuration.asMilli(), operationCount, meanSingleThread, (operationCount / (double) meanSingleThread.asNano()) * 1000000));
        Duration meanSameThread = meanDuration(sameThreadExecutorTimes);
        System.out.println(String.format("Spinner [Sleep = %s ms] (same thread executor) %s ops in %s: %s ops/ms", spinnerSleepDuration.asMilli(), operationCount, meanSameThread, (operationCount / (double) meanSameThread.asNano()) * 1000000));
        System.out.println();
    }

    private Duration meanDuration(List<Duration> durations) {
        long totalAsMilli = 0;
        for (Duration duration : durations) {
            totalAsMilli += duration.asNano();
        }
        return Duration.fromNano(totalAsMilli / durations.size());
    }

    private Duration doTest(Thread thread, ConcurrentErrorReporter errorReporter, ConcurrentMetricsService metricsService, long operationCount) throws MetricsCollectionException {
        TimeSource systemTimeSource = new SystemTimeSource();
        Time benchmarkStartTime = systemTimeSource.now();

        timeSource.setNowFromMilli(1);

        // Note, run() instead of start() to get more precise benchmark numbers
        thread.run();

        Time benchmarkFinishTime = systemTimeSource.now();
        Duration benchmarkDuration = benchmarkFinishTime.durationGreaterThan(benchmarkStartTime);

        assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));

        // wait for all results to get processed by metrics service
        long metricsCollectionTimeoutAsMilli = systemTimeSource.now().plus(Duration.fromSeconds(2)).asMilli();
        while (systemTimeSource.nowAsMilli() < metricsCollectionTimeoutAsMilli && metricsService.results().totalOperationCount() < operationCount) {
            Spinner.powerNap(Time.fromMilli(500).asMilli());
        }
        long numberResultsCollected = metricsService.results().totalOperationCount();
        assertThat(String.format("%s of %s results collected by metrics service", numberResultsCollected, operationCount), numberResultsCollected, is(operationCount));

        return benchmarkDuration;
    }

    private Iterator<Operation<?>> getOperations(long count) {
        Iterator<Time> scheduledStartTimes = gf.constant(Time.fromMilli(1));
        Iterator<Time> dependencyTimes = gf.constant(Time.fromMilli(0));
        Iterator<String> names = gf.constant("name");
        Iterator<Operation<?>> operations = gf.limit(new TimedNamedOperation1Factory(scheduledStartTimes, dependencyTimes, names), count);
        return operations;
    }

    private PreciseIndividualBlockingOperationStreamExecutorServiceThread getNewThread(
            ConcurrentErrorReporter errorReporter,
            WorkloadStreams.WorkloadStreamDefinition streamDefinition,
            Spinner spinner,
            OperationHandlerExecutor operationHandlerExecutor,
            Db db,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            ConcurrentMetricsService metricsService,
            DummyGlobalCompletionTimeReader globalCompletionTimeReader,
            AtomicBoolean executorHasFinished,
            AtomicBoolean forceThreadToTerminate
    ) throws CompletionTimeException, MetricsCollectionException, DbException {
        PreciseIndividualBlockingOperationStreamExecutorServiceThread operationStreamExecutorThread =
                new PreciseIndividualBlockingOperationStreamExecutorServiceThread(
                        timeSource,
                        operationHandlerExecutor,
                        errorReporter,
                        streamDefinition,
                        executorHasFinished,
                        spinner,
                        forceThreadToTerminate,
                        db,
                        localCompletionTimeWriter,
                        globalCompletionTimeReader,
                        metricsService,
                        WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH
                );

        return operationStreamExecutorThread;
    }
}
