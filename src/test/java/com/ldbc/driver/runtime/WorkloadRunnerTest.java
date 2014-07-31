package com.ldbc.driver.runtime;

import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.scheduling.Spinner;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.testutils.ThreadPoolLoadGenerator;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.CsvWritingLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
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
    TimeSource TIME_SOURCE = new SystemTimeSource();
    CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDb()
            throws DbException, WorkloadException, MetricsCollectionException, IOException, CompletionTimeException, InterruptedException {
        Map<String, String> paramsMap = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // Driver-specific parameters
        String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "temp_results_file.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(30);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;

        assertThat(new File(resultFilePath).exists(), is(false));

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, statusDisplayInterval, timeUnit, resultFilePath, timeCompressionRatio, windowedExecutionWindowDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration, printHelp);

        ConcurrentControlService controlService = new LocalControlService(TIME_SOURCE.now().plus(Duration.fromSeconds(5)), configuration);
        Db db = new DummyLdbcSnbInteractiveDb();
        db.init(configuration.asMap());
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(generators, configuration.operationCount());
        Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(operations, controlService.workloadStartTime(), 1.0);
        Map<Class<? extends Operation>, OperationClassification> operationClassifications = workload.operationClassifications();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(
                TIME_SOURCE,
                errorReporter,
                configuration.timeUnit(),
                controlService.workloadStartTime());

        ConcurrentCompletionTimeService concurrentCompletionTimeService =
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
                concurrentCompletionTimeService,
                controlService.configuration().threadCount(),
                controlService.configuration().statusDisplayInterval(),
                controlService.workloadStartTime(),
                controlService.configuration().toleratedExecutionDelay(),
                controlService.configuration().spinnerSleepDuration(),
                controlService.configuration().windowedExecutionWindowDuration(),
                WorkloadRunner.EARLY_SPINNER_OFFSET_DURATION);

        runner.executeWorkload();

        db.cleanup();
        workload.cleanup();
        WorkloadResultsSnapshot workloadResults = metricsService.results();

        assertThat(metricsService.results().startTime().gte(controlService.workloadStartTime()), is(true));
        assertThat(metricsService.results().startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
        assertThat(metricsService.results().latestFinishTime().gt(metricsService.results().startTime()), is(true));

        metricsService.shutdown();
        concurrentCompletionTimeService.shutdown();

        WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

        assertThat(workloadResults, equalTo(workloadResultsFromJson));
        assertThat(workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
    }

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithCsvDbAndReturnExpectedMetrics()
            throws DbException, WorkloadException, MetricsCollectionException, IOException, CompletionTimeException, InterruptedException {
        Map<String, String> paramsMap = LdbcSnbInteractiveWorkload.defaultReadOnlyConfig();
        paramsMap.put(LdbcSnbInteractiveWorkload.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        paramsMap.put(LdbcSnbInteractiveWorkload.DATA_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
        // CsvDb-specific parameters
        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        paramsMap.put(CsvWritingLdbcSnbInteractiveDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String dbClassName = CsvWritingLdbcSnbInteractiveDb.class.getName();
        String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
        long operationCount = 1000;
        int threadCount = 1;
        Duration statusDisplayInterval = Duration.fromSeconds(1);
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "temp_results_file.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        double timeCompressionRatio = 1.0;
        Duration windowedExecutionWindowDuration = Duration.fromSeconds(1);
        Set<String> peerIds = new HashSet<>();
        Duration toleratedExecutionDelay = Duration.fromSeconds(30);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);
        boolean printHelp = false;

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, statusDisplayInterval, timeUnit, resultFilePath, timeCompressionRatio, windowedExecutionWindowDuration, peerIds, toleratedExecutionDelay,
                validationParams, dbValidationFilePath, validateWorkload, calculateWorkloadStatistics, spinnerSleepDuration, printHelp);

        ConcurrentControlService controlService = new LocalControlService(TIME_SOURCE.now().plus(Duration.fromMilli(1000)), configuration);
        Db db = new CsvWritingLdbcSnbInteractiveDb();
        db.init(configuration.asMap());
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init(configuration);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(generators, configuration.operationCount());
        Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(operations, controlService.workloadStartTime(), 1.0);
        Map<Class<? extends Operation>, OperationClassification> operationClassifications = workload.operationClassifications();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(
                TIME_SOURCE,
                errorReporter,
                configuration.timeUnit(),
                controlService.workloadStartTime());

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
                WorkloadRunner.EARLY_SPINNER_OFFSET_DURATION);


        runner.executeWorkload();

        db.cleanup();
        workload.cleanup();
        WorkloadResultsSnapshot workloadResults = metricsService.results();
        metricsService.shutdown();
        concurrentCompletionTimeService.shutdown();

        assertThat(workloadResults.startTime().gte(controlService.workloadStartTime()), is(true));
        assertThat(workloadResults.startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
        assertThat(workloadResults.latestFinishTime().gt(workloadResults.startTime()), is(true));
        assertThat(workloadResults.totalOperationCount(), is(operationCount));

        WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

        assertThat(workloadResults, equalTo(workloadResultsFromJson));
        assertThat(workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
    }
}
