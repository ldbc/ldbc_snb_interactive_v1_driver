package com.ldbc.driver.runtime;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.*;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.*;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.csv.SimpleCsvFileReader;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveConfiguration;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class WorkloadRunnerTest {
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    DecimalFormat numberFormatter = new DecimalFormat("###,###,###,###");
    DecimalFormat doubleNumberFormatter = new DecimalFormat("###,###,###,##0.00");

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final long ONE_SECOND_AS_NANO = TimeUnit.SECONDS.toNanos(1);

    TimeSource timeSource = new SystemTimeSource();
    CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        List<Integer> threadCounts = Lists.newArrayList(1, 2, 4, 8);
        long operationCount = 1000;
        for (int threadCount : threadCounts) {
            doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(threadCount, operationCount);
        }
    }

    public void doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(int threadCount, long operationCount)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        ConcurrentCompletionTimeService completionTimeService = null;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 0.00001;
            Set<String> peerIds = new HashSet<>();
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0l;
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

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")));

            controlService = new LocalControlService(timeSource.nowAsMilli(), configuration);
            db = new DummyLdbcSnbInteractiveDb();
            db.init(configuration.asMap());

            GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            Tuple.Tuple3<WorkloadStreams, Workload, Long> workloadStreamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(configuration, gf);

            workload = workloadStreamsAndWorkload._2();

            WorkloadStreams workloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreamsAndWorkload._1(),
                    controlService.workloadStartTimeAsMilli(),
                    configuration.timeCompressionRatio(),
                    gf
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter = new SimpleCsvFileWriter(resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR);
            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(configuration.asMap())
            );

            ConcurrentCompletionTimeService concurrentCompletionTimeService =
                    completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(
                            controlService.configuration().peerIds());

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    concurrentCompletionTimeService,
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize);

            runner.executeWorkload();

            WorkloadResultsSnapshot workloadResults = metricsService.results();

            SimpleWorkloadMetricsFormatter metricsFormatter = new SimpleWorkloadMetricsFormatter();

            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), workloadResults.startTimeAsMilli() >= controlService.workloadStartTimeAsMilli(), is(true));
            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), workloadResults.latestFinishTimeAsMilli() >= workloadResults.startTimeAsMilli(), is(true));
            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), workloadResults.totalOperationCount(), greaterThanOrEqualTo(operationCount));

            WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

            assertThat(errorReporter.toString(), workloadResults, equalTo(workloadResultsFromJson));
            assertThat(errorReporter.toString(), workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));

            csvResultsLogWriter.close();
            SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
            // NOT + 1 because I didn't add csv headers
            // GREATER THAN or equal because number of Short Reads is operation result-dependent
            assertThat((long) Iterators.size(csvResultsLogReader), greaterThanOrEqualTo(configuration.operationCount()));
            csvResultsLogReader.close();

            double operationsPerSecond = Math.round(((double) operationCount / workloadResults.totalRunDurationAsNano()) * ONE_SECOND_AS_NANO);
            double microSecondPerOperation = (double) TimeUnit.NANOSECONDS.toMicros(workloadResults.totalRunDurationAsNano()) / operationCount;
            System.out.println(
                    String.format("[%s threads] Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            threadCount,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(workloadResults.totalRunDurationAsNano()),
                            doubleNumberFormatter.format(operationsPerSecond),
                            doubleNumberFormatter.format(microSecondPerOperation))
            );
        } finally {
            System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
            if (null != completionTimeService) completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldRunReadWriteLdbcWorkloadWithNothingDbAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        List<Integer> threadCounts = Lists.newArrayList(1, 2, 4, 8);
        long operationCount = 10000;
        for (int threadCount : threadCounts) {
            doShouldRunReadWriteLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(threadCount, operationCount);
        }
    }

    public void doShouldRunReadWriteLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(int threadCount, long operationCount)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        ConcurrentCompletionTimeService completionTimeService = null;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 0.000001;
            Set<String> peerIds = new HashSet<>();
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0l;
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

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")));

            controlService = new LocalControlService(timeSource.nowAsMilli(), configuration);
            db = new DummyLdbcSnbInteractiveDb();
            db.init(configuration.asMap());

            GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            Tuple.Tuple3<WorkloadStreams, Workload, Long> workloadStreamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(configuration, gf);

            workload = workloadStreamsAndWorkload._2();

            WorkloadStreams workloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreamsAndWorkload._1(),
                    controlService.workloadStartTimeAsMilli(),
                    configuration.timeCompressionRatio(),
                    gf
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter = new SimpleCsvFileWriter(resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR);
            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(configuration.asMap())
            );

            ConcurrentCompletionTimeService concurrentCompletionTimeService =
                    completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(
                            controlService.configuration().peerIds());

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    concurrentCompletionTimeService,
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize);

            runner.executeWorkload();

            WorkloadResultsSnapshot workloadResults = metricsService.results();

            SimpleWorkloadMetricsFormatter metricsFormatter = new SimpleWorkloadMetricsFormatter();

            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), errorReporter.errorEncountered(), is(false));
            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), workloadResults.startTimeAsMilli() >= controlService.workloadStartTimeAsMilli(), is(true));
            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), workloadResults.latestFinishTimeAsMilli() >= workloadResults.startTimeAsMilli(), is(true));
            // GREATER THAN or equal because number of Short Reads is operation result-dependent
            assertThat(errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults), workloadResults.totalOperationCount(), greaterThanOrEqualTo(operationCount));

            WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

            assertThat(errorReporter.toString(), workloadResults, equalTo(workloadResultsFromJson));
            assertThat(errorReporter.toString(), workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));

            csvResultsLogWriter.close();
            SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
            // NOT + 1 because I didn't add csv headers
            // GREATER THAN or equal because number of Short Reads is operation result-dependent
            assertThat((long) Iterators.size(csvResultsLogReader), greaterThanOrEqualTo(configuration.operationCount()));
            csvResultsLogReader.close();

            double operationsPerSecond = Math.round(((double) operationCount / workloadResults.totalRunDurationAsNano()) * ONE_SECOND_AS_NANO);
            double microSecondPerOperation = (double) TimeUnit.NANOSECONDS.toMicros(workloadResults.totalRunDurationAsNano()) / operationCount;
            System.out.println(
                    String.format("[%s threads] Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            threadCount,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(workloadResults.totalRunDurationAsNano()),
                            doubleNumberFormatter.format(operationsPerSecond),
                            doubleNumberFormatter.format(microSecondPerOperation))
            );
        } finally {
            System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
            if (null != completionTimeService) completionTimeService.shutdown();
        }
    }

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesUsingSynchronizedCompletionTimeServiceAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        List<Integer> threadCounts = Lists.newArrayList(1, 2, 4);
        long operationCount = 1000000;
        for (int threadCount : threadCounts) {
            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            ConcurrentCompletionTimeService completionTimeService =
                    completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(new HashSet<String>());
            try {
                doShouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics(threadCount, operationCount, completionTimeService, errorReporter);
            } finally {
                completionTimeService.shutdown();
            }
        }
    }

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesUsingThreadedCompletionTimeServiceAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        List<Integer> threadCounts = Lists.newArrayList(1, 2, 4);
        long operationCount = 1000000;
        for (int threadCount : threadCounts) {
            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            ConcurrentCompletionTimeService completionTimeService =
                    completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(new SystemTimeSource(), new HashSet<String>(), new ConcurrentErrorReporter());
            try {
                doShouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics(threadCount, operationCount, completionTimeService, errorReporter);
            } finally {
                completionTimeService.shutdown();
            }
        }
    }

    public void doShouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics(int threadCount,
                                                                                                                     long operationCount,
                                                                                                                     ConcurrentCompletionTimeService completionTimeService,
                                                                                                                     ConcurrentErrorReporter errorReporter)
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException, CompletionTimeException, DriverConfigurationException {
        ConcurrentControlService controlService = null;
        Db db = null;
        Workload workload = null;
        ConcurrentMetricsService metricsService = null;
        try {
            Map<String, String> paramsMap = LdbcSnbInteractiveConfiguration.defaultReadOnlyConfig();
            paramsMap.put(LdbcSnbInteractiveConfiguration.PARAMETERS_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            paramsMap.put(LdbcSnbInteractiveConfiguration.UPDATES_DIRECTORY, TestUtils.getResource("/").getAbsolutePath());
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 1.0;
            Set<String> peerIds = new HashSet<>();
            ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = null;
            String dbValidationFilePath = null;
            boolean validateWorkload = false;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0l;
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

            configuration = (ConsoleAndFileDriverConfiguration) configuration.applyMap(MapUtils.loadPropertiesToMap(TestUtils.getResource("/updateStream.properties")));

            controlService = new LocalControlService(timeSource.nowAsMilli() + 1000, configuration);
            db = new DummyLdbcSnbInteractiveDb();
            db.init(configuration.asMap());
            workload = new LdbcSnbInteractiveWorkload();
            workload.init(configuration);
            GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
            Iterator<Operation<?>> operations = gf.limit(workload.streams(gf).mergeSortedByStartTime(gf), configuration.operationCount());
            Iterator<Operation<?>> timeMappedOperations = gf.timeOffsetAndCompress(operations, controlService.workloadStartTimeAsMilli(), 1.0);
            WorkloadStreams workloadStreams = new WorkloadStreams();
            workloadStreams.setAsynchronousStream(
                    new HashSet<Class<? extends Operation<?>>>(),
                    new HashSet<Class<? extends Operation<?>>>(),
                    Collections.<Operation<?>>emptyIterator(),
                    timeMappedOperations,
                    null
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter = new SimpleCsvFileWriter(resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR);
            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(configuration.asMap())
            );

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize);

            runner.executeWorkload();

            WorkloadResultsSnapshot workloadResults = metricsService.results();
            SimpleWorkloadMetricsFormatter metricsFormatter = new SimpleWorkloadMetricsFormatter();

            assertThat(
                    errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults),
                    errorReporter.errorEncountered(), is(false));
            assertThat(
                    errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults),
                    workloadResults.latestFinishTimeAsMilli() >= workloadResults.startTimeAsMilli(), is(true));
            assertThat(
                    errorReporter.toString() + "\n" + metricsFormatter.format(workloadResults),
                    workloadResults.totalOperationCount(), is(operationCount));

            WorkloadResultsSnapshot workloadResultsFromJson = WorkloadResultsSnapshot.fromJson(workloadResults.toJson());

            assertThat(errorReporter.toString(), workloadResults, equalTo(workloadResultsFromJson));
            assertThat(errorReporter.toString(), workloadResults.toJson(), equalTo(workloadResultsFromJson.toJson()));

            csvResultsLogWriter.close();
            SimpleCsvFileReader csvResultsLogReader = new SimpleCsvFileReader(resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
            assertThat((long) Iterators.size(csvResultsLogReader), is(configuration.operationCount())); // NOT + 1 because I didn't add csv headers
            csvResultsLogReader.close();

            double operationsPerSecond = Math.round(((double) operationCount / workloadResults.totalRunDurationAsNano()) * ONE_SECOND_AS_NANO);
            double microSecondPerOperation = (double) TimeUnit.NANOSECONDS.toMicros(workloadResults.totalRunDurationAsNano()) / operationCount;
            System.out.println(
                    String.format("[%s threads] Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            threadCount,
                            numberFormatter.format(operationCount),
                            TEMPORAL_UTIL.nanoDurationToString(workloadResults.totalRunDurationAsNano()),
                            doubleNumberFormatter.format(operationsPerSecond),
                            doubleNumberFormatter.format(microSecondPerOperation)
                    )
            );
        } finally {
            System.out.println(errorReporter.toString());
            if (null != controlService) controlService.shutdown();
            if (null != db) db.close();
            if (null != workload) workload.close();
            if (null != metricsService) metricsService.shutdown();
            if (null != completionTimeService) completionTimeService.shutdown();
        }
    }
}
