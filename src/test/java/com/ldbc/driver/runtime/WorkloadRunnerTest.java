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
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Ignore;
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

    TimeSource timeSource = new SystemTimeSource();
    CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException {
        boolean ignoreScheduledStartTime = false;
        long operationCount = 1000;
        doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetrics(ignoreScheduledStartTime, operationCount);
    }

    @Ignore
    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException {
        boolean ignoreScheduledStartTime = true;
        long operationCount = 1000;
        doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetrics(ignoreScheduledStartTime, operationCount);
    }

    public void doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetrics(boolean ignoreScheduledStartTime, long operationCount)
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
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
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

            controlService = new LocalControlService(timeSource.now().plus(Duration.fromMilli(1000)), configuration);
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
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    controlService.workloadStartTime(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION,
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_DELAY_DURATION);

            ConcurrentCompletionTimeService concurrentCompletionTimeService =
                    completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(
                            controlService.configuration().peerIds());

            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
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
                    WorkloadRunner.DEFAULT_DURATION_TO_WAIT_FOR_ALL_HANDLERS_TO_FINISH,
                    ignoreScheduledStartTime);


            runner.executeWorkload();

            WorkloadResultsSnapshot workloadResults = metricsService.results();

            assertThat(errorReporter.toString(), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString(), workloadResults.startTime().gte(controlService.workloadStartTime()), is(true));
            assertThat(errorReporter.toString(), workloadResults.startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
            assertThat(errorReporter.toString(), workloadResults.latestFinishTime().gt(workloadResults.startTime()), is(true));
            assertThat(errorReporter.toString(), workloadResults.totalOperationCount(), is(operationCount));

            WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

            assertThat(errorReporter.toString(), workloadResults, equalTo(workloadResultsFromJson));
            assertThat(errorReporter.toString(), workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
        } finally {
            if (null != controlService) controlService.shutdown();
            if (null != db) db.shutdown();
            if (null != workload) workload.cleanup();
            if (null != metricsService) metricsService.shutdown();
            if (null != completionTimeService) completionTimeService.shutdown();
        }
    }
}
