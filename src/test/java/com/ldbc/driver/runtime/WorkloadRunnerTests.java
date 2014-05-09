package com.ldbc.driver.runtime;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.ThreadedQueuedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.db.CsvDb;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.db.NothingDb;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WorkloadRunnerTests {
    @Test
    public void shouldRunLdbcWorkloadWithNothingDb() throws DbException, WorkloadException, MetricsCollectionException, IOException, CompletionTimeException {
        Map<String, String> paramsMap = new HashMap<String, String>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_1_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_2_KEY, "2");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_3_KEY, "3");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_4_KEY, "4");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_5_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_6_KEY, "6");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_7_KEY, "7");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_8_KEY, "6");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_9_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_10_KEY, "4");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_11_KEY, "3");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_12_KEY, "2");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_13_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_14_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json");
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
        paramsMap.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
        // Driver-specific parameters
        String dbClassName = NothingDb.class.getName();
        String workloadClassName = LdbcInteractiveWorkload.class.getName();
        long operationCount = 1000;
        int threadCount = 1;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "temp_results_file.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        Double timeCompressionRatio = null;
        Duration gctDeltaDuration = Duration.fromSeconds(10);
        List<String> peerIds = Lists.newArrayList();
        Duration toleratedExecutionDelay = Duration.fromMilli(100);

        assertThat(new File(resultFilePath).exists(), is(false));

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay);

        ConcurrentControlService controlService = new LocalControlService(Time.now().plus(Duration.fromMilli(1000)), configuration);
        Db db = new NothingDb();
        db.init(configuration.asMap());
        Workload workload = new LdbcInteractiveWorkload();
        workload.init(configuration);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(generators);
        Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(operations, controlService.workloadStartTime(), 1.0);
        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = workload.operationClassifications();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(errorReporter, configuration.timeUnit());
        ConcurrentCompletionTimeService completionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(controlService.configuration().peerIds(), errorReporter);

        WorkloadRunner runner = new WorkloadRunner(controlService, db, timeMappedOperations, operationClassifications, metricsService, errorReporter, completionTimeService);

        runner.executeWorkload();

        db.cleanup();
        workload.cleanup();
        WorkloadResultsSnapshot workloadResults = metricsService.results();

        assertThat(metricsService.results().startTime().gte(controlService.workloadStartTime()), is(true));
        assertThat(metricsService.results().startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
        assertThat(metricsService.results().finishTime().gt(metricsService.results().startTime()), is(true));

        metricsService.shutdown();

        WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

        assertThat(workloadResults, equalTo(workloadResultsFromJson));
        assertThat(workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
    }

    @Test
    public void shouldRunLdbcWorkloadWithCsvDbAndReturnExpectedMetrics() throws DbException, WorkloadException, MetricsCollectionException, IOException, CompletionTimeException {
        Map<String, String> paramsMap = new HashMap<String, String>();
        // LDBC Interactive Workload-specific parameters
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_1_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_2_KEY, "2");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_3_KEY, "3");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_4_KEY, "4");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_5_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_6_KEY, "6");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_7_KEY, "7");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_8_KEY, "6");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_9_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_10_KEY, "4");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_11_KEY, "3");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_12_KEY, "2");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_13_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.READ_OPERATION_14_KEY, "1");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_1_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_2_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_3_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_4_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_5_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_6_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_7_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_OPERATION_8_KEY, "false");
        paramsMap.put(LdbcInteractiveWorkload.PARAMETERS_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/parameters.json");
        paramsMap.put(LdbcInteractiveWorkload.INTERLEAVE_DURATION_KEY, "5");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_STREAM_FILENAME_KEY, "ldbc_driver/workloads/ldbc/socnet/interactive/updates.csv");
        paramsMap.put(LdbcInteractiveWorkload.WRITE_RATIO_KEY, "0");
        paramsMap.put(LdbcInteractiveWorkload.READ_RATIO_KEY, "1");
        // CsvDb-specific parameters
        String csvOutputFilePath = "temp_csv_output_file.csv";
        FileUtils.deleteQuietly(new File(csvOutputFilePath));
        paramsMap.put(CsvDb.CSV_PATH_KEY, csvOutputFilePath);
        // Driver-specific parameters
        String dbClassName = CsvDb.class.getName();
        String workloadClassName = LdbcInteractiveWorkload.class.getName();
        long operationCount = 1000;
        int threadCount = 4;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultFilePath = "temp_results_file.json";
        FileUtils.deleteQuietly(new File(resultFilePath));
        Double timeCompressionRatio = null;
        Duration gctDeltaDuration = Duration.fromMinutes(1);
        List<String> peerIds = Lists.newArrayList();
        Duration toleratedExecutionDelay = Duration.fromMilli(1000);

        assertThat(new File(csvOutputFilePath).exists(), is(false));
        assertThat(new File(resultFilePath).exists(), is(false));

        ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(paramsMap, dbClassName, workloadClassName, operationCount,
                threadCount, showStatus, timeUnit, resultFilePath, timeCompressionRatio, gctDeltaDuration, peerIds, toleratedExecutionDelay);

        ConcurrentControlService controlService = new LocalControlService(Time.now().plus(Duration.fromMilli(1000)), configuration);
        Db db = new CsvDb();
        db.init(configuration.asMap());
        Workload workload = new LdbcInteractiveWorkload();
        workload.init(configuration);
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterator<Operation<?>> operations = workload.operations(generators);
        Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(operations, controlService.workloadStartTime(), 1.0);
        Map<Class<? extends Operation<?>>, OperationClassification> operationClassifications = workload.operationClassifications();
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentMetricsService metricsService = new ThreadedQueuedConcurrentMetricsService(errorReporter, configuration.timeUnit());
        ConcurrentCompletionTimeService completionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(controlService.configuration().peerIds(), errorReporter);

        WorkloadRunner runner = new WorkloadRunner(controlService, db, timeMappedOperations, operationClassifications, metricsService, errorReporter, completionTimeService);

        runner.executeWorkload();

        db.cleanup();
        workload.cleanup();
        WorkloadResultsSnapshot workloadResults = metricsService.results();
        metricsService.shutdown();

        assertThat(workloadResults.startTime().gte(controlService.workloadStartTime()), is(true));
        assertThat(workloadResults.startTime().lt(controlService.workloadStartTime().plus(configuration.toleratedExecutionDelay())), is(true));
        assertThat(workloadResults.finishTime().gt(workloadResults.startTime()), is(true));
        assertThat(workloadResults.totalOperationCount(), is(operationCount));

        WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

        assertThat(workloadResults, equalTo(workloadResultsFromJson));
        assertThat(workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));
    }
}
