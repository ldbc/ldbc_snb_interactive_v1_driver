package com.ldbc.driver.runtime.executor;

import com.google.common.collect.Lists;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationClassification;
import com.ldbc.driver.OperationClassification.SchedulingMode;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DummyCountingConcurrentMetricsService;
import com.ldbc.driver.runtime.DummyGlobalCompletionTimeReader;
import com.ldbc.driver.runtime.DummyLocalCompletionTimeWriter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.*;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1;
import com.ldbc.driver.workloads.dummy.TimedNamedOperation1Factory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

@Ignore
public class OperationStreamExecutorPerformanceTest {
    private final ManualTimeSource TIME_SOURCE = new ManualTimeSource(0);
    private final GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

    /*
Mirko,
I just pushed a change to the driver should improve performance of update query execution.
here are results from some micro benchmarks I performed on the change:

[Sleep = 0 ms] OLD: 1575.299306868305 ops/ms
[Sleep = 0 ms] NEW: 2012.477359629704 ops/ms

[Sleep = 1 ms] OLD: 2.218859417505026 ops/ms
[Sleep = 1 ms] NEW: 2032.5203252032522 ops/ms

[Sleep = 10 ms] OLD: 0.2262034020991676 ops/ms
[Sleep = 10 ms] NEW: 2127.6595744680853 ops/ms

CONFIGURATION/ASSUMPTIONS OF TEST:
 - query dependencies are always fulfilled (queries never need to wait on their dependencies)
 - queries are always

Given the assumption above, the new implementation performs at the same speed regardless of
     */

    /*
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

    public void synchronousExecutorPerformanceTestWithSpinnerDuration(Duration spinnerSleepDuration, int experimentRepetitions, long operationCount) throws CompletionTimeException, MetricsCollectionException, DbException, OperationHandlerExecutorException {
        List<Duration> newThreadOldExecutorTimes = new ArrayList<>();
        List<Duration> newThreadNewExecutorTimes = new ArrayList<>();

        List<Operation<?>> operations = Lists.newArrayList(getOperations(operationCount));

        while (experimentRepetitions-- > 0) {
            // New Thread Old Executor
            {
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                        TIME_SOURCE,
                        Duration.fromMilli(10),
                        errorReporter);
                Spinner spinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy);
                Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
                operationClassifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.NONE));
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
                TIME_SOURCE.setNowFromMilli(0);

                OperationHandlerExecutor executor = new ThreadPoolOperationHandlerExecutor(2);
                PreciseIndividualBlockingOperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        operations.iterator(),
                        spinner,
                        executor,
                        operationClassifications,
                        db,
                        localCompletionTimeWriter,
                        metricsService,
                        globalCompletionTimeReader,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                newThreadOldExecutorTimes.add(doTest(thread, errorReporter, metricsService, operationCount));
                executor.shutdown(Duration.fromSeconds(1));
                db.shutdown();
                metricsService.shutdown();
            }
            // New Thread New Executor
            {
                ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
                ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                        TIME_SOURCE,
                        Duration.fromMilli(10),
                        errorReporter);
                Spinner spinner = new Spinner(TIME_SOURCE, spinnerSleepDuration, executionDelayPolicy);
                Map<Class<? extends Operation>, OperationClassification> operationClassifications = new HashMap<>();
                operationClassifications.put(TimedNamedOperation1.class, new OperationClassification(SchedulingMode.INDIVIDUAL_BLOCKING, OperationClassification.DependencyMode.NONE));
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
                TIME_SOURCE.setNowFromMilli(0);

                OperationHandlerExecutor executor = new SingleThreadOperationHandlerExecutor(errorReporter);
                PreciseIndividualBlockingOperationStreamExecutorServiceThread thread = getNewThread(
                        errorReporter,
                        operations.iterator(),
                        spinner,
                        executor,
                        operationClassifications,
                        db,
                        localCompletionTimeWriter,
                        metricsService,
                        globalCompletionTimeReader,
                        executorHasFinished,
                        forceThreadToTerminate
                );

                newThreadNewExecutorTimes.add(doTest(thread, errorReporter, metricsService, operationCount));
                executor.shutdown(Duration.fromSeconds(1));
                db.shutdown();
                metricsService.shutdown();
            }
        }

        Duration meanNewOld = meanDuration(newThreadOldExecutorTimes);
        System.out.println(String.format("Spinner [Sleep = %s ms] (NEW thread OLD executor) %s ops in %s: %s ops/ms", spinnerSleepDuration.asMilli(), operationCount, meanNewOld, (operationCount / (double) meanNewOld.asNano()) * 1000000));
        Duration meanNewNew = meanDuration(newThreadNewExecutorTimes);
        System.out.println(String.format("Spinner [Sleep = %s ms] (NEW thread NEW executor) %s ops in %s: %s ops/ms", spinnerSleepDuration.asMilli(), operationCount, meanNewNew, (operationCount / (double) meanNewNew.asNano()) * 1000000));
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

        TIME_SOURCE.setNowFromMilli(1);

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
            Iterator<Operation<?>> operations,
            Spinner spinner,
            OperationHandlerExecutor operationHandlerExecutor,
            Map<Class<? extends Operation>, OperationClassification> operationClassifications,
            Db db,
            LocalCompletionTimeWriter localCompletionTimeWriter,
            ConcurrentMetricsService metricsService,
            DummyGlobalCompletionTimeReader globalCompletionTimeReader,
            AtomicBoolean executorHasFinished,
            AtomicBoolean forceThreadToTerminate
    ) throws CompletionTimeException, MetricsCollectionException, DbException {
        PreciseIndividualBlockingOperationStreamExecutorServiceThread operationStreamExecutorThread = new PreciseIndividualBlockingOperationStreamExecutorServiceThread(
                TIME_SOURCE,
                operationHandlerExecutor,
                errorReporter,
                operations,
                executorHasFinished,
                spinner,
                spinner,
                forceThreadToTerminate,
                operationClassifications,
                db,
                localCompletionTimeWriter,
                globalCompletionTimeReader,
                metricsService
        );

        return operationStreamExecutorThread;
    }
}
