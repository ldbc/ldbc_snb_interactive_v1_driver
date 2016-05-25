package com.ldbc.driver.client;

import com.google.common.base.Charsets;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeService;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.DisruptorSbeMetricsService;
import com.ldbc.driver.runtime.metrics.JsonWorkloadMetricsFormatter;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.MetricsManager;
import com.ldbc.driver.runtime.metrics.MetricsService;
import com.ldbc.driver.runtime.metrics.WorkloadResultsSnapshot;
import com.ldbc.driver.runtime.metrics.WorkloadStatusSnapshot;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.validation.ResultsLogValidationResult;
import com.ldbc.driver.validation.ResultsLogValidationSummary;
import com.ldbc.driver.validation.ResultsLogValidationTolerances;
import com.ldbc.driver.validation.ResultsLogValidator;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.DecimalFormat;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ExecuteWorkloadMode implements ClientMode<Object>
{
    private static final DecimalFormat NUMBER_FORMAT = new DecimalFormat( "###,###,###,###,###" );
    private final ControlService controlService;
    private final TimeSource timeSource;
    private final LoggingService loggingService;
    private final long randomSeed;
    private final TemporalUtil temporalUtil;
    private final ResultsDirectory resultsDirectory;

    private Workload workload = null;
    private Db database = null;
    private MetricsService metricsService = null;
    private CompletionTimeService completionTimeService = null;
    private WorkloadRunner workloadRunner = null;
    private SimpleCsvFileWriter csvResultsLogFileWriter = null;

    public ExecuteWorkloadMode(
            ControlService controlService,
            TimeSource timeSource,
            long randomSeed ) throws ClientException
    {
        this.controlService = controlService;
        this.timeSource = timeSource;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
        this.randomSeed = randomSeed;
        this.temporalUtil = new TemporalUtil();
        this.resultsDirectory = new ResultsDirectory( controlService.configuration() );
    }

    /*
    TODO clientMode.init()
    TODO clientMode
     */
    public WorkloadStatusSnapshot status() throws MetricsCollectionException
    {
        // TODO
        return null;
    }

    @Override
    public void init() throws ClientException
    {
        loggingService.info( "Driver Configuration" );
        loggingService.info( controlService.toString() );
    }

    @Override
    public Object startExecutionAndAwaitCompletion() throws ClientException
    {
        if ( controlService.configuration().skipCount() > 0 )
        {
            loggingService.info(
                    format( "\n --- First %s operations will be skipped ---",
                            NUMBER_FORMAT.format( controlService.configuration().skipCount() ) ) );
        }
        if ( controlService.configuration().warmupCount() > 0 )
        {
            loggingService.info( "\n" +
                                 " --------------------\n" +
                                 " --- Warmup Phase ---\n" +
                                 " --------------------" );
            doInit( true );
            doExecute( true );
            try
            {
                // TODO remove in future
                // This is necessary to clear the runnable context pool
                // As objects in the pool would otherwise hold references to services used during warmup
                database.reInit();
            }
            catch ( DbException e )
            {
                throw new ClientException(
                        format( "Error reinitializing DB after warmup: %s", database.getClass().getName() ), e );
            }
        }
        else
        {
            loggingService.info( "\n" +
                                 " ---------------------------------\n" +
                                 " --- No Warmup Phase Requested ---\n" +
                                 " ---------------------------------" );
        }

        loggingService.info( "\n" +
                             " -----------------\n" +
                             " --- Run Phase ---\n" +
                             " -----------------" );
        doInit( false );
        doExecute( false );

        try
        {
            loggingService.info( "Shutting down database connector..." );
            database.close();
            loggingService.info( "Database connector shutdown successfully" );
        }
        catch ( IOException e )
        {
            throw new ClientException( "Error shutting down database", e );
        }
        loggingService.info( "Workload completed successfully" );
        return null;
    }

    private void doInit( boolean warmup ) throws ClientException
    {
        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( randomSeed ) );

        //  ================================
        //  ===  Results Log CSV Writer  ===
        //  ================================
        File resultsLog = resultsDirectory.getOrCreateResultsLogFile( warmup );
        if ( null != resultsLog )
        {
            try
            {
                csvResultsLogFileWriter = new SimpleCsvFileWriter(
                        resultsLog,
                        SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR
                );

                csvResultsLogFileWriter.writeRow(
                        "operation_type",
                        "scheduled_start_time_" + TimeUnit.MILLISECONDS.name(),
                        "actual_start_time_" + TimeUnit.MILLISECONDS.name(),
                        "execution_duration_" + controlService.configuration().timeUnit().name(),
                        "result_code",
                        "original_start_time"
                );
            }
            catch ( IOException e )
            {
                throw new ClientException(
                        format( "Error while creating results log file: %s", resultsLog.getAbsolutePath() ), e
                );
            }
        }

        //  ==================
        //  ===  Workload  ===
        //  ==================
        loggingService.info( "Scanning workload streams to calculate their limits..." );

        long offset = (warmup)
                      ? controlService.configuration().skipCount()
                      : controlService.configuration().skipCount() + controlService.configuration().warmupCount();
        long limit = (warmup)
                     ? controlService.configuration().warmupCount()
                     : controlService.configuration().operationCount();

        WorkloadStreams workloadStreams;
        long minimumTimeStamp;
        try
        {
            boolean returnStreamsWithDbConnector = true;
            Tuple3<WorkloadStreams,Workload,Long> streamsAndWorkloadAndMinimumTimeStamp =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            controlService.configuration(),
                            gf,
                            returnStreamsWithDbConnector,
                            offset,
                            limit,
                            controlService.loggingServiceFactory()
                    );
            workloadStreams = streamsAndWorkloadAndMinimumTimeStamp._1();
            workload = streamsAndWorkloadAndMinimumTimeStamp._2();
            minimumTimeStamp = streamsAndWorkloadAndMinimumTimeStamp._3();
        }
        catch ( Exception e )
        {
            throw new ClientException( format( "Error loading workload class: %s",
                    controlService.configuration().workloadClassName() ), e );
        }
        loggingService.info( format( "Loaded workload: %s", workload.getClass().getName() ) );

        loggingService.info( format( "Retrieving workload stream: %s", workload.getClass().getSimpleName() ) );
        controlService.setWorkloadStartTimeAsMilli( System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 5 ) );
        WorkloadStreams timeMappedWorkloadStreams;
        try
        {
            timeMappedWorkloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                    workloadStreams,
                    controlService.workloadStartTimeAsMilli(),
                    controlService.configuration().timeCompressionRatio(),
                    gf
            );
        }
        catch ( WorkloadException e )
        {
            throw new ClientException( "Error while retrieving operation stream for workload", e );
        }

        //  ================
        //  =====  DB  =====
        //  ================
        if ( null == database )
        {
            try
            {
                database = ClassLoaderHelper.loadDb( controlService.configuration().dbClassName() );
                database.init(
                        controlService.configuration().asMap(),
                        controlService.loggingServiceFactory().loggingServiceFor( database.getClass().getSimpleName() ),
                        workload.operationTypeToClassMapping()
                );
            }
            catch ( DbException e )
            {
                throw new ClientException(
                        format( "Error loading DB class: %s", controlService.configuration().dbClassName() ), e );
            }
            loggingService.info( format( "Loaded DB: %s", database.getClass().getName() ) );
        }

        //  ========================
        //  ===  Metrics Service  ==
        //  ========================
        try
        {
            // TODO create metrics service factory so different ones can be easily created
            metricsService = new DisruptorSbeMetricsService(
                    timeSource,
                    errorReporter,
                    controlService.configuration().timeUnit(),
                    DisruptorSbeMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    csvResultsLogFileWriter,
                    workload.operationTypeToClassMapping(),
                    controlService.loggingServiceFactory()
            );
        }
        catch ( MetricsCollectionException e )
        {
            throw new ClientException( "Error creating metrics service", e );
        }

        //  =================================
        //  ===  Completion Time Service  ===
        //  =================================
        CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
        try
        {
            completionTimeService =
                    completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(
                            timeSource,
                            controlService.configuration().peerIds(),
                            errorReporter
                    );
        }
        catch ( CompletionTimeException e )
        {
            throw new ClientException(
                    format( "Error while instantiating Completion Time Service with peer IDs %s",
                            controlService.configuration().peerIds().toString() ), e );
        }

        //  ========================
        //  ===  Workload Runner  ==
        //  ========================
        loggingService.info( format( "Instantiating %s", WorkloadRunner.class.getSimpleName() ) );
        try
        {
            int operationHandlerExecutorsBoundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
            workloadRunner = new WorkloadRunner(
                    timeSource,
                    database,
                    timeMappedWorkloadStreams,
                    metricsService,
                    errorReporter,
                    completionTimeService,
                    controlService.loggingServiceFactory(),
                    controlService.configuration().threadCount(),
                    controlService.configuration().statusDisplayIntervalAsSeconds(),
                    controlService.configuration().spinnerSleepDurationAsMilli(),
                    controlService.configuration().ignoreScheduledStartTimes(),
                    operationHandlerExecutorsBoundedQueueSize );
        }
        catch ( Exception e )
        {
            throw new ClientException( format( "Error instantiating %s", WorkloadRunner.class.getSimpleName() ), e );
        }

        //  ===========================================
        //  ===  Initialize Completion Time Service  ==
        //  ===========================================
        // TODO note, this MUST be done after creation of Workload Runner because Workload Runner creates the
        // TODO "writers" for completion time service (refactor this mess at some stage)
        try
        {
            if ( completionTimeService.getAllWriters().isEmpty() )
            {
                // There are no local completion time writers, GCT would never advance or be non-null,
                // set to max so nothing ever waits on it
                long nearlyMaxPossibleTimeAsMilli = Long.MAX_VALUE - 1;
                long maxPossibleTimeAsMilli = Long.MAX_VALUE;
                // Create a writer to use for advancing GCT
                LocalCompletionTimeWriter localCompletionTimeWriter =
                        completionTimeService.newLocalCompletionTimeWriter();
                localCompletionTimeWriter.submitLocalInitiatedTime( nearlyMaxPossibleTimeAsMilli );
                localCompletionTimeWriter.submitLocalCompletedTime( nearlyMaxPossibleTimeAsMilli );
                localCompletionTimeWriter.submitLocalInitiatedTime( maxPossibleTimeAsMilli );
                localCompletionTimeWriter.submitLocalCompletedTime( maxPossibleTimeAsMilli );
            }
            else
            {
                // There are some local completion time writers, initialize them to lowest time stamp in workload
                completionTimeServiceAssistant
                        .writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, minimumTimeStamp - 1 );
                completionTimeServiceAssistant
                        .writeInitiatedAndCompletedTimesToAllWriters( completionTimeService, minimumTimeStamp );
                boolean globalCompletionTimeAdvancedToDesiredTime =
                        completionTimeServiceAssistant.waitForGlobalCompletionTime(
                                timeSource,
                                minimumTimeStamp - 1,
                                TimeUnit.SECONDS.toMillis( 5 ),
                                completionTimeService,
                                errorReporter
                        );
                long globalCompletionTimeWaitTimeoutDurationAsMilli = TimeUnit.SECONDS.toMillis( 5 );
                if ( !globalCompletionTimeAdvancedToDesiredTime )
                {
                    throw new ClientException(
                            format(
                                    "Timed out [%s] while waiting for global completion time to advance to workload " +
                                    "start time\nCurrent GCT: %s\nWaiting For GCT: %s",
                                    globalCompletionTimeWaitTimeoutDurationAsMilli,
                                    completionTimeService.globalCompletionTimeAsMilli(),
                                    controlService.workloadStartTimeAsMilli() )
                    );
                }
                loggingService.info( "GCT: " + temporalUtil
                        .milliTimeToDateTimeString( completionTimeService.globalCompletionTimeAsMilli() ) + " / " +
                                     completionTimeService.globalCompletionTimeAsMilli() );
            }
        }
        catch ( CompletionTimeException e )
        {
            throw new ClientException(
                    "Error while writing initial initiated and completed times to Completion Time Service", e );
        }
    }

    private void doExecute( boolean warmup ) throws ClientException
    {
        try
        {
            ConcurrentErrorReporter errorReporter = workloadRunner.getFuture().get();
            loggingService.info( "Shutting down workload..." );
            workload.close();
            if ( errorReporter.errorEncountered() )
            {
                throw new ClientException( "Error running workload\n" + errorReporter.toString() );
            }
        }
        catch ( Exception e )
        {
            throw new ClientException( "Error running workload", e );
        }

        loggingService.info( "Shutting down completion time service..." );
        try
        {
            completionTimeService.shutdown();
        }
        catch ( CompletionTimeException e )
        {
            throw new ClientException( "Error during shutdown of completion time service", e );
        }

        loggingService.info( "Shutting down metrics collection service..." );
        WorkloadResultsSnapshot workloadResults;
        try
        {
            workloadResults = metricsService.getWriter().results();
            metricsService.shutdown();
        }
        catch ( MetricsCollectionException e )
        {
            throw new ClientException( "Error during shutdown of metrics collection service", e );
        }

        try
        {
            if ( warmup )
            {
                loggingService.summaryResult( workloadResults );
            }
            else
            {
                loggingService.detailedResult( workloadResults );
            }
            if ( resultsDirectory.exists() )
            {
                File resultsSummaryFile = resultsDirectory.getOrCreateResultsSummaryFile( warmup );
                loggingService.info(
                        format( "Exporting workload metrics to %s...", resultsSummaryFile.getAbsolutePath() )
                );
                MetricsManager.export( workloadResults,
                        new JsonWorkloadMetricsFormatter(),
                        new FileOutputStream( resultsSummaryFile ),
                        Charsets.UTF_8
                );
                File configurationFile = resultsDirectory.getOrCreateConfigurationFile( warmup );
                Files.write(
                        configurationFile.toPath(),
                        controlService.configuration().toPropertiesString().getBytes( StandardCharsets.UTF_8 )
                );
                csvResultsLogFileWriter.close();
                if ( !controlService.configuration().ignoreScheduledStartTimes() )
                {
                    loggingService.info( "Validating workload results..." );
                    // TODO make this feature accessible directly
                    ResultsLogValidator resultsLogValidator = new ResultsLogValidator();
                    ResultsLogValidationTolerances resultsLogValidationTolerances =
                            workload.resultsLogValidationTolerances(
                                    controlService.configuration(),
                                    warmup
                            );
                    ResultsLogValidationSummary resultsLogValidationSummary = resultsLogValidator.compute(
                            resultsDirectory.getOrCreateResultsLogFile( warmup ),
                            resultsLogValidationTolerances.excessiveDelayThresholdAsMilli()
                    );
                    File resultsValidationFile = resultsDirectory.getOrCreateResultsValidationFile( warmup );
                    loggingService.info(
                            format( "Exporting workload results validation to: %s",
                                    resultsValidationFile.getAbsolutePath() )
                    );
                    Files.write(
                            resultsValidationFile.toPath(),
                            resultsLogValidationSummary.toJson().getBytes( StandardCharsets.UTF_8 )
                    );
                    // TODO export result
                    ResultsLogValidationResult validationResult = resultsLogValidator.validate(
                            resultsLogValidationSummary,
                            resultsLogValidationTolerances
                    );
                    loggingService.info( validationResult.toString() );
                    Files.write(
                            resultsValidationFile.toPath(),
                            resultsLogValidationSummary.toJson().getBytes( StandardCharsets.UTF_8 )
                    );
                }
            }
        }
        catch ( Exception e )
        {
            throw new ClientException( "Could not export workload metrics", e );
        }
    }
}
