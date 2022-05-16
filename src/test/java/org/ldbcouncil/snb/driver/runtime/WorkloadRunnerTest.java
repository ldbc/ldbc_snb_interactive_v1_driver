package org.ldbcouncil.snb.driver.runtime;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.WorkloadStreams;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.ControlService;
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.control.LocalControlService;
import org.ldbcouncil.snb.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.control.LoggingServiceFactory;
import org.ldbcouncil.snb.driver.csv.simple.SimpleCsvFileReader;
import org.ldbcouncil.snb.driver.csv.simple.SimpleCsvFileWriter;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeException;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeService;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeServiceAssistant;
import org.ldbcouncil.snb.driver.runtime.metrics.MetricsCollectionException;
import org.ldbcouncil.snb.driver.runtime.metrics.MetricsService;
import org.ldbcouncil.snb.driver.runtime.metrics.SimpleDetailedWorkloadMetricsFormatter;
import org.ldbcouncil.snb.driver.runtime.metrics.ThreadedQueuedMetricsService;
import org.ldbcouncil.snb.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.snb.driver.temporal.SystemTimeSource;
import org.ldbcouncil.snb.driver.temporal.TemporalUtil;
import org.ldbcouncil.snb.driver.temporal.TimeSource;
import org.ldbcouncil.snb.driver.testutils.TestUtils;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.util.Tuple3;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcQuery4;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class WorkloadRunnerTest
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private static final LoggingServiceFactory LOGGING_SERVICE_FACTORY = new Log4jLoggingServiceFactory( false );
    private DecimalFormat numberFormatter = new DecimalFormat( "###,###,###,###" );
    private DecimalFormat doubleNumberFormatter = new DecimalFormat( "###,###,###,##0.00" );

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();
    private static final long ONE_SECOND_AS_NANO = TimeUnit.SECONDS.toNanos( 1 );

    private TimeSource timeSource = new SystemTimeSource();
    private CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbAndCrashInSaneManner()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        int threadCount = 4;
        long operationCount = 100000;

        ControlService controlService = null;
        Db db = null;
        Workload workload = null;
        MetricsService metricsService = null;
        CompletionTimeService completionTimeService = null;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        try
        {
            Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultReadOnlyConfigSF1();
            paramsMap.put(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 0.0000001;
            String dbValidationFilePath = null;
            boolean validationCreationParams = false;
            boolean validationSerializationCheck = false;
            int validationParamsSize = 0;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0L;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = false;
            long warmupCount = 100;
            long skipCount = 10;
            boolean flushLog = false;

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
                    validationCreationParams,
                    validationParamsSize,
                    validationSerializationCheck,
                    dbValidationFilePath,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( MapUtils.loadPropertiesToMap( TestUtils.getResource(
                            "/snb/interactive/updateStream.properties" ) ) );

            controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );

            GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
            boolean returnStreamsWithDbConnector = true;
            Tuple3<WorkloadStreams,Workload,Long> workloadStreamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            configuration,
                            gf,
                            returnStreamsWithDbConnector,
                            configuration.warmupCount(),
                            configuration.operationCount(),
                            LOGGING_SERVICE_FACTORY
                    );

            workload = workloadStreamsAndWorkload._2();

            WorkloadStreams workloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreamsAndWorkload._1(),
                    controlService.workloadStartTimeAsMilli(),
                    configuration.timeCompressionRatio(),
                    gf
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter =
                    new SimpleCsvFileWriter( resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR, flushLog );
            metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(),
                    LOGGING_SERVICE_FACTORY
            );

            completionTimeService = completionTimeServiceAssistant.newSynchronizedCompletionTimeService();

            db = new DummyLdbcSnbInteractiveDb();
            db.init(
                    configuration
                            .applyArg( DummyLdbcSnbInteractiveDb.CRASH_ON_ARG, LdbcQuery4.class.getName() )
                            .asMap(),
                    loggingService,
                    workload.operationTypeToClassMapping()
            );

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.loggingServiceFactory(),
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize );

            runner.getFuture().get();
            csvResultsLogWriter.close();
        }
        finally
        {
            try
            {
                controlService.shutdown();
            }
            catch ( Throwable e )
            {
                System.out.println( format( "Unclean %s shutdown -- but it's OK",
                        controlService.getClass().getSimpleName() ) );
            }
            try
            {
                db.close();
            }
            catch ( Throwable e )
            {
                System.out.println( format( "Unclean %s shutdown -- but it's OK",
                        db.getClass().getSimpleName() ) );
            }
            try
            {
                workload.close();
            }
            catch ( Throwable e )
            {
                System.out.println( format( "Unclean %s shutdown -- but it's OK",
                        workload.getClass().getSimpleName() ) );
            }
            try
            {
                metricsService.shutdown();
            }
            catch ( Throwable e )
            {
                System.out.println( format( "Unclean %s shutdown -- but it's OK",
                        metricsService.getClass().getSimpleName() ) );
            }
            try
            {
                completionTimeService.shutdown();
            }
            catch ( Throwable e )
            {
                System.out.println( format( "Unclean %s shutdown -- but it's OK",
                        completionTimeService.getClass().getSimpleName() ) );
            }
            System.out.println( errorReporter.toString() );
            assertTrue( errorReporter.errorEncountered() );
        }
    }

    @Test
    public void shouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        List<Integer> threadCounts = Lists.newArrayList( 1, 2, 4, 8 );
        long operationCount = 100000;
        for ( int threadCount : threadCounts )
        {
            doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(
                    threadCount,
                    operationCount
            );
        }
    }

    private void doShouldRunReadOnlyLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(
            int threadCount, long operationCount )
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        ControlService controlService = null;
        Db db = null;
        Workload workload = null;
        MetricsService metricsService = null;
        CompletionTimeService completionTimeService = null;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        try
        {
            Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultReadOnlyConfigSF1();
            paramsMap.put(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 0.0000001;
            String dbValidationFilePath = null;
            boolean validationCreationParams = false;
            boolean validationSerializationCheck = false;
            int validationParamsSize = 0;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0L;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = false;
            long warmupCount = 100;
            long skipCount = 10;
            boolean flushLog = false;

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
                    validationCreationParams,
                    validationParamsSize,
                    validationSerializationCheck,
                    dbValidationFilePath,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( MapUtils.loadPropertiesToMap( TestUtils.getResource(
                            "/snb/interactive/updateStream.properties" ) ) );

            controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );

            GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
            boolean returnStreamsWithDbConnector = true;
            Tuple3<WorkloadStreams,Workload,Long> workloadStreamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            configuration,
                            gf,
                            returnStreamsWithDbConnector,
                            configuration.warmupCount(),
                            configuration.operationCount(),
                            LOGGING_SERVICE_FACTORY
                    );

            workload = workloadStreamsAndWorkload._2();

            WorkloadStreams workloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreamsAndWorkload._1(),
                    controlService.workloadStartTimeAsMilli(),
                    configuration.timeCompressionRatio(),
                    gf
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter =
                    new SimpleCsvFileWriter( resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR, flushLog );
            metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(),
                    LOGGING_SERVICE_FACTORY
            );

            completionTimeService = completionTimeServiceAssistant.newSynchronizedCompletionTimeService();

            db = new DummyLdbcSnbInteractiveDb();
            db.init(
                    configuration.asMap(),
                    loggingService,
                    workload.operationTypeToClassMapping()
            );

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.loggingServiceFactory(),
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize );

            runner.getFuture().get();

            WorkloadResultsSnapshot workloadResults = metricsService.getWriter().results();

            SimpleDetailedWorkloadMetricsFormatter metricsFormatter = new SimpleDetailedWorkloadMetricsFormatter();

            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    errorReporter.errorEncountered(), is( false ) );
            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.startTimeAsMilli() >= controlService.workloadStartTimeAsMilli(), is( true ) );
            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.latestFinishTimeAsMilli() >= workloadResults.startTimeAsMilli(), is( true ) );
            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.totalOperationCount(),
                    allOf( greaterThanOrEqualTo( percent( operationCount, 0.9 ) ),
                            lessThanOrEqualTo( percent( operationCount, 1.1 ) ) )
            );

            WorkloadResultsSnapshot workloadResultsFromJson =
                    WorkloadResultsSnapshot.fromJson( workloadResults.toJson() );

            assertThat( errorReporter.toString(), workloadResults, equalTo( workloadResultsFromJson ) );
            assertThat( errorReporter.toString(), workloadResults.toJson(),
                    equalTo( workloadResultsFromJson.toJson() ) );

            csvResultsLogWriter.close();
            SimpleCsvFileReader csvResultsLogReader =
                    new SimpleCsvFileReader( resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
            // NOT + 1 because I didn't add csv headers
            // GREATER THAN or equal because number of Short Reads is operation result-dependent
            assertThat(
                    (long) Iterators.size( csvResultsLogReader ),
                    allOf( greaterThanOrEqualTo( percent( configuration.operationCount(), 0.9 ) ),
                            lessThanOrEqualTo( percent( configuration.operationCount(), 1.1 ) ) )
            );
            csvResultsLogReader.close();

            operationCount = metricsService.getWriter().results().totalOperationCount();
            double operationsPerSecond = Math.round(
                    ((double) operationCount / workloadResults.totalRunDurationAsNano()) * ONE_SECOND_AS_NANO );
            double microSecondPerOperation =
                    (double) TimeUnit.NANOSECONDS.toMicros( workloadResults.totalRunDurationAsNano() ) / operationCount;
            System.out.println(
                    format( "[%s threads] Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            threadCount,
                            numberFormatter.format( operationCount ),
                            TEMPORAL_UTIL.nanoDurationToString( workloadResults.totalRunDurationAsNano() ),
                            doubleNumberFormatter.format( operationsPerSecond ),
                            doubleNumberFormatter.format( microSecondPerOperation ) )
            );
        }
        finally
        {
            System.out.println( errorReporter.toString() );
            if ( null != controlService )
            {
                controlService.shutdown();
            }
            if ( null != db )
            {
                db.close();
            }
            if ( null != workload )
            {
                workload.close();
            }
            if ( null != metricsService )
            {
                metricsService.shutdown();
            }
            if ( null != completionTimeService )
            {
                completionTimeService.shutdown();
            }
        }
    }

    @Test
    public void shouldRunReadWriteLdbcWorkloadWithNothingDbAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        List<Integer> threadCounts = Lists.newArrayList( 1, 2, 4, 8 );
        long operationCount = 10000;
        for ( int threadCount : threadCounts )
        {
            doShouldRunReadWriteLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(
                    threadCount,
                    operationCount
            );
        }
    }

    public void doShouldRunReadWriteLdbcWorkloadWithNothingDbAndReturnExpectedMetricsIncludingResultsLog(
            int threadCount, long operationCount )
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        ControlService controlService = null;
        Db db = null;
        Workload workload = null;
        MetricsService metricsService = null;
        CompletionTimeService completionTimeService = null;
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        try
        {
            Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 0.000001;
            String dbValidationFilePath = null;
            boolean validationCreationParams = false;
            boolean validationSerializationCheck = false;
            int validationParamsSize = 0;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0L;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = false;
            long warmupCount = 100;
            long skipCount = 10;
            boolean flushLog = false;

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
                    validationCreationParams,
                    validationParamsSize,
                    validationSerializationCheck,
                    dbValidationFilePath,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( MapUtils.loadPropertiesToMap( TestUtils.getResource(
                            "/snb/interactive/updateStream.properties" ) ) );

            controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );

            GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
            boolean returnStreamsWithDbConnector = true;
            Tuple3<WorkloadStreams,Workload,Long> workloadStreamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            configuration,
                            gf,
                            returnStreamsWithDbConnector,
                            configuration.warmupCount(),
                            configuration.operationCount(),
                            LOGGING_SERVICE_FACTORY
                    );

            workload = workloadStreamsAndWorkload._2();

            WorkloadStreams workloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreamsAndWorkload._1(),
                    controlService.workloadStartTimeAsMilli(),
                    configuration.timeCompressionRatio(),
                    gf
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter =
                    new SimpleCsvFileWriter( resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR, flushLog );
            metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(),
                    LOGGING_SERVICE_FACTORY
            );

            db = new DummyLdbcSnbInteractiveDb();
            db.init(
                    configuration.asMap(),
                    loggingService,
                    workload.operationTypeToClassMapping()
            );

            completionTimeService = completionTimeServiceAssistant.newSynchronizedCompletionTimeService();

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.loggingServiceFactory(),
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize );

            runner.getFuture().get();

            WorkloadResultsSnapshot workloadResults = metricsService.getWriter().results();

            SimpleDetailedWorkloadMetricsFormatter metricsFormatter = new SimpleDetailedWorkloadMetricsFormatter();

            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    errorReporter.errorEncountered(), is( false ) );
            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.startTimeAsMilli() >= controlService.workloadStartTimeAsMilli(), is( true ) );
            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.latestFinishTimeAsMilli() >= workloadResults.startTimeAsMilli(), is( true ) );
            // GREATER THAN or equal because number of Short Reads is operation result-dependent
            assertThat( errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.totalOperationCount(),
                    allOf( greaterThanOrEqualTo( percent( operationCount, 0.9 ) ),
                            lessThanOrEqualTo( percent( operationCount, 1.1 ) ) )
            );

            WorkloadResultsSnapshot workloadResultsFromJson =
                    WorkloadResultsSnapshot.fromJson( workloadResults.toJson() );

            assertThat( errorReporter.toString(), workloadResults, equalTo( workloadResultsFromJson ) );
            assertThat( errorReporter.toString(), workloadResults.toJson(),
                    equalTo( workloadResultsFromJson.toJson() ) );

            csvResultsLogWriter.close();
            SimpleCsvFileReader csvResultsLogReader =
                    new SimpleCsvFileReader( resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
            // NOT + 1 because I didn't add csv headers
            // GREATER THAN or equal because number of Short Reads is operation result-dependent
            assertThat(
                    (long) Iterators.size( csvResultsLogReader ),
                    allOf( greaterThanOrEqualTo( percent( configuration.operationCount(), 0.9 ) ),
                            lessThanOrEqualTo( percent( configuration.operationCount(), 1.1 ) ) )
            );
            csvResultsLogReader.close();

            operationCount = metricsService.getWriter().results().totalOperationCount();
            double operationsPerSecond = Math.round(
                    ((double) operationCount / workloadResults.totalRunDurationAsNano()) * ONE_SECOND_AS_NANO );
            double microSecondPerOperation =
                    (double) TimeUnit.NANOSECONDS.toMicros( workloadResults.totalRunDurationAsNano() ) / operationCount;
            System.out.println(
                    format( "[%s threads] Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            threadCount,
                            numberFormatter.format( operationCount ),
                            TEMPORAL_UTIL.nanoDurationToString( workloadResults.totalRunDurationAsNano() ),
                            doubleNumberFormatter.format( operationsPerSecond ),
                            doubleNumberFormatter.format( microSecondPerOperation ) )
            );
        }
        finally
        {
            System.out.println( errorReporter.toString() );
            if ( null != controlService )
            {
                controlService.shutdown();
            }
            if ( null != db )
            {
                db.close();
            }
            if ( null != workload )
            {
                workload.close();
            }
            if ( null != metricsService )
            {
                metricsService.shutdown();
            }
            if ( null != completionTimeService )
            {
                completionTimeService.shutdown();
            }
        }
    }

    @Test
    public void
    shouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesUsingSynchronizedCompletionTimeServiceAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        List<Integer> threadCounts = Lists.newArrayList( 1, 2, 4 );
        long operationCount = 1000000;
        for ( int threadCount : threadCounts )
        {
            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            CompletionTimeService completionTimeService =
                    completionTimeServiceAssistant.newSynchronizedCompletionTimeService();
            try
            {
                doShouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics(
                        threadCount,
                        operationCount,
                        completionTimeService,
                        errorReporter
                );
            }
            finally
            {
                completionTimeService.shutdown();
            }
        }
    }

    @Test
    public void
    shouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesUsingThreadedCompletionTimeServiceAndReturnExpectedMetrics()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        List<Integer> threadCounts = Lists.newArrayList( 1, 2, 4 );
        long operationCount = 1000000;
        for ( int threadCount : threadCounts )
        {
            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
            CompletionTimeService completionTimeService =
                    completionTimeServiceAssistant.newThreadedQueuedCompletionTimeService(
                            new SystemTimeSource(),
                            new ConcurrentErrorReporter() );
            try
            {
                doShouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics(
                        threadCount,
                        operationCount,
                        completionTimeService,
                        errorReporter
                );
            }
            finally
            {
                completionTimeService.shutdown();
            }
        }
    }

    private void doShouldRunReadOnlyLdbcWorkloadWithNothingDbWhileIgnoringScheduledStartTimesAndReturnExpectedMetrics(
            int threadCount,
            long operationCount,
            CompletionTimeService completionTimeService,
            ConcurrentErrorReporter errorReporter )
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException, ExecutionException
    {
        ControlService controlService = null;
        Db db = null;
        Workload workload = null;
        MetricsService metricsService = null;
        try
        {
            Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultReadOnlyConfigSF1();
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath() );
            // Driver-specific parameters
            String name = null;
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 1;
            TimeUnit timeUnit = TimeUnit.NANOSECONDS;
            String resultDirPath = temporaryFolder.newFolder().getAbsolutePath();
            double timeCompressionRatio = 1.0;
            String dbValidationFilePath = null;
            boolean validationCreationParams = false;
            boolean validationSerializationCheck = false;
            int validationParamsSize = 0;
            boolean calculateWorkloadStatistics = false;
            long spinnerSleepDuration = 0L;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = true;
            long warmupCount = 100;
            long skipCount = 10;
            boolean flushLog = false;

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
                    validationCreationParams,
                    validationParamsSize,
                    validationSerializationCheck,
                    dbValidationFilePath,
                    calculateWorkloadStatistics,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( MapUtils.loadPropertiesToMap( TestUtils.getResource(
                            "/snb/interactive/updateStream.properties" ) ) );

            controlService = new LocalControlService(
                    timeSource.nowAsMilli() + 1000,
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            LoggingService loggingService = new Log4jLoggingServiceFactory( false ).loggingServiceFor( "Test" );
            workload = new LdbcSnbInteractiveWorkload();
            workload.init( configuration );
            db = new DummyLdbcSnbInteractiveDb();
            db.init(
                    configuration.asMap(),
                    loggingService,
                    workload.operationTypeToClassMapping()
            );
            GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( 42L ) );
            Iterator<Operation> operations = gf.limit(
                    WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators( gf,
                            workload.streams( gf, true ) ),
                    configuration.operationCount()
            );
            Iterator<Operation> timeMappedOperations =
                    gf.timeOffsetAndCompress( operations, controlService.workloadStartTimeAsMilli(), 1.0 );
            WorkloadStreams workloadStreams = new WorkloadStreams();
            workloadStreams.setAsynchronousStream(
                    new HashSet<Class<? extends Operation>>(),
                    new HashSet<Class<? extends Operation>>(),
                    Collections.<Operation>emptyIterator(),
                    timeMappedOperations,
                    null
            );

            File resultsLog = temporaryFolder.newFile();
            SimpleCsvFileWriter csvResultsLogWriter =
                    new SimpleCsvFileWriter( resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR, flushLog );
            metricsService = ThreadedQueuedMetricsService.newInstanceUsingBlockingBoundedQueue(
                    timeSource,
                    errorReporter,
                    configuration.timeUnit(),
                    ThreadedQueuedMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogWriter,
                    workload.operationTypeToClassMapping(),
                    LOGGING_SERVICE_FACTORY
            );

            int boundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            WorkloadRunner runner = new WorkloadRunner(
                    timeSource,
                    db,
                    workloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.loggingServiceFactory(),
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    boundedQueueSize );

            runner.getFuture().get();

            WorkloadResultsSnapshot workloadResults = metricsService.getWriter().results();
            SimpleDetailedWorkloadMetricsFormatter metricsFormatter = new SimpleDetailedWorkloadMetricsFormatter();

            assertThat(
                    errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    errorReporter.errorEncountered(), is( false ) );
            assertThat(
                    errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.latestFinishTimeAsMilli() >= workloadResults.startTimeAsMilli(), is( true ) );
            assertThat(
                    errorReporter.toString() + "\n" + metricsFormatter.format( workloadResults ),
                    workloadResults.totalOperationCount(), is( operationCount ) );

            WorkloadResultsSnapshot workloadResultsFromJson =
                    WorkloadResultsSnapshot.fromJson( workloadResults.toJson() );

            assertThat( errorReporter.toString(), workloadResults, equalTo( workloadResultsFromJson ) );
            assertThat( errorReporter.toString(), workloadResults.toJson(),
                    equalTo( workloadResultsFromJson.toJson() ) );

            csvResultsLogWriter.close();
            SimpleCsvFileReader csvResultsLogReader =
                    new SimpleCsvFileReader( resultsLog, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
            assertThat( (long) Iterators.size( csvResultsLogReader ),
                    is( configuration.operationCount() ) ); // NOT + 1 because I didn't add csv headers
            csvResultsLogReader.close();

            operationCount = metricsService.getWriter().results().totalOperationCount();
            double operationsPerSecond = Math.round(
                    ((double) operationCount / workloadResults.totalRunDurationAsNano()) * ONE_SECOND_AS_NANO );
            double microSecondPerOperation =
                    (double) TimeUnit.NANOSECONDS.toMicros( workloadResults.totalRunDurationAsNano() ) / operationCount;
            System.out.println(
                    format( "[%s threads] Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            threadCount,
                            numberFormatter.format( operationCount ),
                            TEMPORAL_UTIL.nanoDurationToString( workloadResults.totalRunDurationAsNano() ),
                            doubleNumberFormatter.format( operationsPerSecond ),
                            doubleNumberFormatter.format( microSecondPerOperation )
                    )
            );
        }
        finally
        {
            System.out.println( errorReporter.toString() );
            if ( null != controlService )
            {
                controlService.shutdown();
            }
            if ( null != db )
            {
                db.close();
            }
            if ( null != workload )
            {
                workload.close();
            }
            if ( null != metricsService )
            {
                metricsService.shutdown();
            }
            if ( null != completionTimeService )
            {
                completionTimeService.shutdown();
            }
        }
    }

    private long percent( long value, double percent )
    {
        return Math.round( value * percent );
    }
}
