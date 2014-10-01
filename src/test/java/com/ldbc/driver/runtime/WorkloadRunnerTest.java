package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.workloads.dummy.DummyDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadRunnerTest {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource TIME_SOURCE = new SystemTimeSource();
    CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDb()
            throws DbException, WorkloadException, MetricsCollectionException, IOException, CompletionTimeException, InterruptedException {
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        ConcurrentCompletionTimeService completionTimeService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
            paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            // Driver-specific parameters
            String name = "name";
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            long operationCount = 1000;
            int threadCount = 1;
            Duration statusDisplayInterval = Duration.fromSeconds(1);
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 1.0;
            Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
            Set<String> peerIds = new HashSet<>();
            Duration toleratedExecutionDelay = Duration.fromMinutes(60);
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            Duration spinnerSleepDuration = Duration.fromMilli(0);
            boolean printHelp = false;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, name, dbClassName, workloadClassName, operationCount,
                    threadCount, statusDisplayInterval, timeUnit, resultDirPath, timeCompressionRatio, windowedExecutionWindowDuration, peerIds, toleratedExecutionDelay,
                    validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration, printHelp);

            controlService = new LocalControlService(TIME_SOURCE.now().plus(Duration.fromSeconds(5)), configuration);
            db = new DummyLdbcSnbInteractiveDb();
            db.init(configuration.asMap());
            workload = new LdbcSnbInteractiveWorkload();
            workload.init(configuration);
            GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            Iterator<Operation<?>> operations = workload.operations(generators, configuration.operationCount());
            Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(operations, controlService.workloadStartTime(), 1.0);
            Map<Class<? extends Operation>, OperationClassification> operationClassifications = workload.operationClassifications();
            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                    TIME_SOURCE,
                    errorReporter,
                    configuration.timeUnit(),
                    controlService.workloadStartTime(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION);

            completionTimeService =
                    completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(
                            TIME_SOURCE,
                            controlService.configuration().peerIds(),
                            errorReporter);

            WorkloadRunner runner = new WorkloadRunner(
                    TIME_SOURCE,
                    db,
                    timeMappedOperations,
                    operationClassifications,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayInterval(),
                    controlService.workloadStartTime(),
                    controlService.configuration().toleratedExecutionDelay(),
                    controlService.configuration().spinnerSleepDuration(),
                    controlService.configuration().windowedExecutionWindowDuration(),
                    WorkloadRunner.DEFAULT_EARLY_SPINNER_OFFSET_DURATION,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH);

            runner.executeWorkload();

            WorkloadResultsSnapshot workloadResults = metricsService.results();

            assertThat(metricsService.results().startTime().gte(controlService.workloadStartTime()), is(true));
            assertThat(metricsService.results().startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
            assertThat(metricsService.results().latestFinishTime().gt(metricsService.results().startTime()), is(true));

            WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

            assertThat(workloadResults, equalTo(workloadResultsFromJson));
            assertThat(workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
        } finally {
            if (null != controlService) controlService.shutdown();
            if (null != db) db.shutdown();
            if (null != workload) workload.cleanup();
            if (null != metricsService) metricsService.shutdown();
            if (null != completionTimeService) completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldRunReadOnlyLdbcWorkloadAndReturnExpectedMetrics()
            throws DbException, WorkloadException, MetricsCollectionException, IOException, CompletionTimeException, InterruptedException {
        // TODO expose test to a lot of load to try to create race condition
        // TODO some handlers have no completed yet, sometimes results is not 100, check where race condition is

        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        ConcurrentCompletionTimeService completionTimeService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
            paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            long operationCount = 1000;
            int threadCount = 1;
            Duration statusDisplayInterval = Duration.fromSeconds(1);
            TimeUnit timeUnit = TimeUnit.MILLISECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 1.0;
            Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
            Set<String> peerIds = new HashSet<>();
            Duration toleratedExecutionDelay = Duration.fromMinutes(60);
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            Duration spinnerSleepDuration = Duration.fromMilli(0);
            boolean printHelp = false;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, name, dbClassName, workloadClassName, operationCount,
                    threadCount, statusDisplayInterval, timeUnit, resultDirPath, timeCompressionRatio, windowedExecutionWindowDuration, peerIds, toleratedExecutionDelay,
                    validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration, printHelp);

            controlService = new LocalControlService(TIME_SOURCE.now().plus(Duration.fromMilli(1000)), configuration);
            db = new DummyDb();
            db.init(configuration.asMap());
            workload = new LdbcSnbInteractiveWorkload();
            workload.init(configuration);
            GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            Iterator<Operation<?>> operations = workload.operations(generators, configuration.operationCount());
            Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(operations, controlService.workloadStartTime(), 1.0);
            Map<Class<? extends Operation>, OperationClassification> operationClassifications = workload.operationClassifications();
            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                    TIME_SOURCE,
                    errorReporter,
                    configuration.timeUnit(),
                    controlService.workloadStartTime(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION);

            ConcurrentCompletionTimeService concurrentCompletionTimeService =
                    completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(
                            controlService.configuration().peerIds());

            WorkloadRunner runner = new WorkloadRunner(
                    TIME_SOURCE,
                    db,
                    timeMappedOperations,
                    operationClassifications,
                    metricsService,
                    errorReporter,
                    concurrentCompletionTimeService,
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayInterval(),
                    controlService.workloadStartTime(),
                    controlService.configuration().toleratedExecutionDelay(),
                    controlService.configuration().spinnerSleepDuration(),
                    controlService.configuration().windowedExecutionWindowDuration(),
                    WorkloadRunner.DEFAULT_EARLY_SPINNER_OFFSET_DURATION,
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH);


            runner.executeWorkload();

            WorkloadResultsSnapshot workloadResults = metricsService.results();

            assertThat(workloadResults.startTime().gte(controlService.workloadStartTime()), is(true));
            assertThat(workloadResults.startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
            assertThat(workloadResults.latestFinishTime().gt(workloadResults.startTime()), is(true));
            assertThat(workloadResults.totalOperationCount(), is(operationCount));

            WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

            assertThat(workloadResults, equalTo(workloadResultsFromJson));
            assertThat(workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
        } finally {
            if (null != controlService) controlService.shutdown();
            if (null != db) db.shutdown();
            if (null != workload) workload.cleanup();
            if (null != metricsService) metricsService.shutdown();
            if (null != completionTimeService) completionTimeService.shutdown();
        }
    }
}
