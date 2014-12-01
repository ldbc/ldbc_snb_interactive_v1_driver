package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class LdbcSnbInteractiveWorkloadPerformanceTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    TimeSource timeSource = new SystemTimeSource();

    @Ignore
    @Test
    public void ignoreTimesPerformanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data/sf10-256/");
        File paramsDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data/sf10-256/");
        List<File> streamsDirs = Lists.newArrayList(
                parentStreamsDir
        );

        for (File streamDir : streamsDirs) {
            List<Integer> threadCounts = Lists.newArrayList(1);
            long operationCount = 1000000;
            for (int threadCount : threadCounts) {
                doIgnoreTimesPerformanceTest(
                        threadCount,
                        operationCount,
                        streamDir.getAbsolutePath(),
                        paramsDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File(streamDir, "updateStream.properties").getAbsolutePath()
                );
            }
        }
    }

    public void doIgnoreTimesPerformanceTest(int threadCount,
                                             long operationCount,
                                             String updateStreamsDir,
                                             String parametersDir,
                                             String resultsDir,
                                             String name,
                                             String updateStreamPropertiesPath)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, parametersDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, updateStreamsDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATE_STREAM_PARSER, LdbcSnbInteractiveConfiguration.UpdateStreamParser.CHAR_SEEKER.name());
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MICROSECONDS.toNanos(100)));
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name());
            // Driver-specific parameters
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 2;
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
            double timeCompressionRatio = 0.0000001;
            Set<String> peerIds = new HashSet<>();
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = true;
            boolean shouldCreateResultsLog = true;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    peerIds,
                    validationParams,
                    dbValidationFilePath,
                    validateWorkload,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    shouldCreateResultsLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(new File(updateStreamPropertiesPath)));

            // When
            Client client = new Client(new LocalControlService(timeSource.nowAsMilli() + 3000, configuration), timeSource);
            client.start();

            // Then
            File resultsFile = new File(resultsDir, name + "-results.json");
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson(resultsFile);

            double operationsPerSecond = Math.round(((double) operationCount / resultsSnapshot.totalRunDurationAsNano()) * TEMPORAL_UTIL.convert(1, TimeUnit.SECONDS, TimeUnit.NANOSECONDS));
            double microSecondPerOperation = (double) TEMPORAL_UTIL.convert(resultsSnapshot.totalRunDurationAsNano(), TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS) / operationCount;
            DecimalFormat numberFormatter = new DecimalFormat("###,###,###,###");
            System.out.println(
                    String.format("[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            name,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(resultsSnapshot.totalRunDurationAsNano()),
                            numberFormatter.format(operationsPerSecond),
                            microSecondPerOperation
                    )
            );
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (errorReporter.errorEncountered())
                System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
        }
    }

    @Ignore
    @Test
    public void withTimesPerformanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        File parentStreamsDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data/sf10-016/");
        File paramsDir = new File("/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data/sf10-016/");
        List<File> streamsDirs = Lists.newArrayList(
                parentStreamsDir
        );

        for (File streamDir : streamsDirs) {
            List<Integer> threadCounts = Lists.newArrayList(1);
            long operationCount = 10000000;
            for (int threadCount : threadCounts) {
                doWithTimesPerformanceTest(
                        threadCount,
                        operationCount,
                        streamDir.getAbsolutePath(),
                        paramsDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File(streamDir, "updateStream.properties").getAbsolutePath()
                );
            }
        }
    }

    public void doWithTimesPerformanceTest(int threadCount,
                                           long operationCount,
                                           String updateStreamsDir,
                                           String parametersDir,
                                           String resultsDir,
                                           String name,
                                           String updateStreamPropertiesPath)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        try {
//            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultWriteOnlyConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, parametersDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, updateStreamsDir);
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATE_STREAM_PARSER, LdbcSnbInteractiveConfiguration.UpdateStreamParser.CHAR_SEEKER.name());
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG, Long.toString(TimeUnit.MICROSECONDS.toNanos(100)));
            paramsMap.put(DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name());
            // Driver-specific parameters
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 2;
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
            double timeCompressionRatio = 0.00001;
            Set<String> peerIds = new HashSet<>();
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = false;
            boolean shouldCreateResultsLog = true;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    peerIds,
                    validationParams,
                    dbValidationFilePath,
                    validateWorkload,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    shouldCreateResultsLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(new File(updateStreamPropertiesPath)));

            // When
            Client client = new Client(new LocalControlService(timeSource.nowAsMilli(), configuration), timeSource);
            client.start();

            // Then
            File resultsFile = new File(resultsDir, name + "-results.json");
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson(resultsFile);

            double operationsPerSecond = Math.round(((double) operationCount / resultsSnapshot.totalRunDurationAsNano()) * TEMPORAL_UTIL.convert(1, TimeUnit.SECONDS, TimeUnit.NANOSECONDS));
            double microSecondPerOperation = (double) TEMPORAL_UTIL.convert(resultsSnapshot.totalRunDurationAsNano(), TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS) / operationCount;
            DecimalFormat numberFormatter = new DecimalFormat("###,###,###,###");
            System.out.println(
                    String.format("[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            name,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(resultsSnapshot.totalRunDurationAsNano()),
                            numberFormatter.format(operationsPerSecond),
                            microSecondPerOperation
                    )
            );
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            if (errorReporter.errorEncountered())
                System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
        }
    }
}
