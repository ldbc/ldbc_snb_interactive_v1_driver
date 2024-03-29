package org.ldbcouncil.snb.driver.workloads.interactive.performance;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Client;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.DbException;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadException;
import org.ldbcouncil.snb.driver.client.ClientMode;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.ControlService;
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.control.LocalControlService;
import org.ldbcouncil.snb.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.snb.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.snb.driver.runtime.coordination.CompletionTimeException;
import org.ldbcouncil.snb.driver.runtime.metrics.MetricsCollectionException;
import org.ldbcouncil.snb.driver.runtime.metrics.MetricsService;
import org.ldbcouncil.snb.driver.runtime.metrics.WorkloadResultsSnapshot;
import org.ldbcouncil.snb.driver.temporal.SystemTimeSource;
import org.ldbcouncil.snb.driver.temporal.TemporalUtil;
import org.ldbcouncil.snb.driver.temporal.TimeSource;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkloadConfiguration;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class InteractiveWorkloadPerformanceTest
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private TimeSource timeSource = new SystemTimeSource();

    @Ignore
    @Test
    public void ignoreTimesPerformanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException
    {
        File parentStreamsDir = new File(
                "/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data/sf10" +
                "-256/" );
        File paramsDir = new File(
                "/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data/sf10" +
                "-256/" );
        List<File> streamsDirs = Lists.newArrayList(
                parentStreamsDir
        );

        for ( File streamDir : streamsDirs )
        {
            List<Integer> threadCounts = Lists.newArrayList( 1 );
            long operationCount = 1000000;
            for ( int threadCount : threadCounts )
            {
                doIgnoreTimesPerformanceTest(
                        threadCount,
                        operationCount,
                        streamDir.getAbsolutePath(),
                        paramsDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File( streamDir, "snb/interactive/updateStream.properties" ).getAbsolutePath()
                );
            }
        }
    }

    private void doIgnoreTimesPerformanceTest( int threadCount,
            long operationCount,
            String updateStreamsDir,
            String parametersDir,
            String resultsDir,
            String name,
            String updateStreamPropertiesPath )
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ControlService controlService = null;
        Db db = null;
        Workload workload = null;
        MetricsService metricsService = null;
        try
        {
            Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY, parametersDir );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY, updateStreamsDir );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_STREAM_PARSER,
                    LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.CHAR_SEEKER.name() );
            paramsMap.put( DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG,
                    Long.toString( TimeUnit.MICROSECONDS.toNanos( 100 ) ) );
            paramsMap.put( DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name() );
            // Driver-specific parameters
            String mode = "execute_benchmark";
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 2;
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
            double timeCompressionRatio = 0.0000001;
            String dbValidationFilePath = null;
            boolean validationSerializationCheck = false;
            int validationParamsSize = 0;
            long spinnerSleepDuration = 0;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = true;
            boolean recordDelayedOperations = true;
            long warmupCount = 0;
            long skipCount = 0;
            boolean flushLog = false;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    mode,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    validationParamsSize,
                    validationSerializationCheck,
                    recordDelayedOperations,
                    dbValidationFilePath,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( MapUtils.loadPropertiesToMap( new File( updateStreamPropertiesPath ) ) );

            // When
            Client client = new Client();
            controlService = new LocalControlService(
                    timeSource.nowAsMilli() + 3000,
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            ClientMode clientMode = client.getClientModeFor( controlService );
            clientMode.init();
            clientMode.startExecutionAndAwaitCompletion();

            // Then
            File resultsFile = new File( resultsDir, name + "-results.json" );
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson( resultsFile );

            double operationsPerSecond = Math.round(
                    ((double) operationCount / resultsSnapshot.totalRunDurationAsNano()) *
                    TimeUnit.SECONDS.toNanos( 1 ) );
            double microSecondPerOperation =
                    (double) TimeUnit.NANOSECONDS.toMicros( resultsSnapshot.totalRunDurationAsNano() ) / operationCount;
            DecimalFormat numberFormatter = new DecimalFormat( "###,###,###,###" );
            System.out.println(
                    format( "[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            name,
                            numberFormatter.format( operationCount ),
                            TEMPORAL_UTIL.nanoDurationToString( resultsSnapshot.totalRunDurationAsNano() ),
                            numberFormatter.format( operationsPerSecond ),
                            microSecondPerOperation
                    )
            );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( errorReporter.errorEncountered() )
            {
                System.out.println( errorReporter.toString() );
            }
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
        }
    }

    @Ignore
    @Test
    public void withTimesPerformanceTest()
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException
    {
        File parentStreamsDir = new File(
                "/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data" +
                "/sf010_p006/" );
        File paramsDir = new File(
                "/Users/alexaverbuch/IdeaProjects/ldbc_snb_workload_interactive_neo4j/ldbc_driver/sample_data" +
                "/sf010_p006/" );
        List<File> streamsDirs = Lists.newArrayList(
                parentStreamsDir
        );

        for ( File streamDir : streamsDirs )
        {
            List<Integer> threadCounts = Lists.newArrayList( 4 );
            long operationCount = 50000000;
            for ( int threadCount : threadCounts )
            {
                doWithTimesPerformanceTest(
                        threadCount,
                        operationCount,
                        streamDir.getAbsolutePath(),
                        paramsDir.getAbsolutePath(),
                        streamDir.getAbsolutePath(),
                        "TC" + threadCount + "-" + streamDir.getName(),
                        new File( streamDir, "snb/interactive/updateStream.properties" ).getAbsolutePath()
                );
            }
        }
    }

    public void doWithTimesPerformanceTest( int threadCount,
            long operationCount,
            String updateStreamsDir,
            String parametersDir,
            String resultsDir,
            String name,
            String updateStreamPropertiesPath )
            throws InterruptedException, DbException, WorkloadException, IOException, MetricsCollectionException,
            CompletionTimeException, DriverConfigurationException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        ControlService controlService = null;
        Db db = null;
        Workload workload = null;
        MetricsService metricsService = null;
        try
        {
            Map<String,String> paramsMap = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY, parametersDir );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY, updateStreamsDir );
            paramsMap.put( LdbcSnbInteractiveWorkloadConfiguration.UPDATE_STREAM_PARSER,
                    LdbcSnbInteractiveWorkloadConfiguration.UpdateStreamParser.CHAR_SEEKER.name() );
            paramsMap.put( DummyLdbcSnbInteractiveDb.SLEEP_DURATION_NANO_ARG,
                    Long.toString( TimeUnit.MICROSECONDS.toNanos( 0 ) ) );
            paramsMap.put( DummyLdbcSnbInteractiveDb.SLEEP_TYPE_ARG, DummyLdbcSnbInteractiveDb.SleepType.SPIN.name() );
            // Driver-specific parameters
            String mode = "execute_benchmark";
            String dbClassName = DummyLdbcSnbInteractiveDb.class.getName();
            String workloadClassName = LdbcSnbInteractiveWorkload.class.getName();
            int statusDisplayInterval = 2;
            TimeUnit timeUnit = TimeUnit.MICROSECONDS;
            String resultDirPath = resultsDir;
            double timeCompressionRatio = 0.0000001;
            String dbValidationFilePath = null;
            boolean validationSerializationCheck = false;
            int validationParamsSize = 0;
            long spinnerSleepDuration = 0;
            boolean printHelp = false;
            boolean ignoreScheduledStartTimes = false;
            boolean recordDelayedOperations = true;
            long warmupCount = 0;
            long skipCount = 0;
            boolean flushLog = false;

            ConsoleAndFileDriverConfiguration configuration = new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    mode,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayInterval,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    validationParamsSize,
                    validationSerializationCheck,
                    recordDelayedOperations,
                    dbValidationFilePath,
                    spinnerSleepDuration,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );

            configuration = (ConsoleAndFileDriverConfiguration) configuration
                    .applyArgs( MapUtils.loadPropertiesToMap( new File( updateStreamPropertiesPath ) ) );

            // When
            Client client = new Client();
            controlService = new LocalControlService(
                    timeSource.nowAsMilli(),
                    configuration,
                    new Log4jLoggingServiceFactory( false ),
                    timeSource
            );
            ClientMode clientMode = client.getClientModeFor( controlService );
            clientMode.init();
            clientMode.startExecutionAndAwaitCompletion();

            // Then
            File resultsFile = new File( resultsDir, name + "-results.json" );
            WorkloadResultsSnapshot resultsSnapshot = WorkloadResultsSnapshot.fromJson( resultsFile );

            double actualOperationCount = resultsSnapshot.totalOperationCount();
            double operationsPerSecond = Math.round(
                    (actualOperationCount / resultsSnapshot.totalRunDurationAsNano()) * TimeUnit.SECONDS.toNanos( 1 ) );
            double microSecondPerOperation =
                    (double) TimeUnit.NANOSECONDS.toMicros( resultsSnapshot.totalRunDurationAsNano() ) /
                    actualOperationCount;
            DecimalFormat numberFormatter = new DecimalFormat( "###,###,###,###" );
            System.out.println(
                    format( "[%s]Completed %s operations in %s = %s op/sec = 1 op/%s us",
                            name,
                            numberFormatter.format( actualOperationCount ),
                            TEMPORAL_UTIL.nanoDurationToString( resultsSnapshot.totalRunDurationAsNano() ),
                            numberFormatter.format( operationsPerSecond ),
                            microSecondPerOperation
                    )
            );
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( errorReporter.errorEncountered() )
            {
                System.out.println( errorReporter.toString() );
            }
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
        }
    }
}
