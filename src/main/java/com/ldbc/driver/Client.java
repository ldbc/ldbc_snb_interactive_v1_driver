package com.ldbc.driver;

import com.google.common.base.Charsets;
import com.ldbc.driver.control.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.DefaultQueues;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.*;
import com.ldbc.driver.runtime.scheduling.ErrorReportingTerminatingExecutionDelayPolicy;
import com.ldbc.driver.runtime.scheduling.ExecutionDelayPolicy;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TemporalUtil;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.csv.SimpleCsvFileReader;
import com.ldbc.driver.util.csv.SimpleCsvFileWriter;
import com.ldbc.driver.validation.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

// TODO replace log4j with some interface like StatusReportingService

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();

    private static final long RANDOM_SEED = 42;

    public static void main(String[] args) throws ClientException {
        TemporalUtil temporalUtil = new TemporalUtil();
        ConcurrentControlService controlService = null;
        try {
            TimeSource systemTimeSource = new SystemTimeSource();
            ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(args);
            // TODO this method will not work with multiple processes - should come from controlService
            long workloadStartTimeAsMilli = systemTimeSource.nowAsMilli() + temporalUtil.convert(10, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
            controlService = new LocalControlService(workloadStartTimeAsMilli, configuration);
            Client client = new Client(controlService, systemTimeSource);
            client.start();
        } catch (DriverConfigurationException e) {
            String errMsg = String.format("Error parsing parameters: %s", e.getMessage());
            logger.error(errMsg);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Client terminated unexpectedly\n" + ConcurrentErrorReporter.stackTraceToString(e));
            System.exit(1);
        } finally {
            if (null != controlService) controlService.shutdown();
        }
    }

    // check results
    private DbValidationResult databaseValidationResult = null;
    private WorkloadValidationResult workloadValidationResult = null;
    private WorkloadStatistics workloadStatistics = null;

    private final ClientMode clientMode;

    // TODO should not be doing things like ConsoleAndFileDriverConfiguration.DB_ARG
    // TODO ConsoleAndFileDriverConfiguration could maybe have a DriverParam(enum)-to-String(arg) method?
    public Client(ConcurrentControlService controlService, TimeSource timeSource) throws ClientException {
        if (controlService.configuration().shouldPrintHelpString()) {
            // Print Help
            clientMode = new PrintHelpMode(controlService);
        } else if (null != controlService.configuration().validationParamsCreationOptions()) {
            // Create Validation Parameters
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if (null == configuration.dbClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.DB_ARG);
            if (null == configuration.workloadClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
            if (0 == configuration.operationCount())
                missingParams.add(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG);
            if (false == missingParams.isEmpty())
                throw new ClientException(String.format("Missing required parameters: %s", missingParams.toString()));
            clientMode = new CreateValidationParamsMode(controlService);
        } else if (null != controlService.configuration().databaseValidationFilePath()) {
            // Validate Database
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if (null == configuration.dbClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.DB_ARG);
            if (null == configuration.workloadClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
            if (false == missingParams.isEmpty())
                throw new ClientException(String.format("Missing required parameters: %s", missingParams.toString()));
            clientMode = new ValidateDatabaseMode(controlService);
        } else if (controlService.configuration().validateWorkload()) {
            // Validate Workload
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if (null == configuration.workloadClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
            if (0 == configuration.operationCount())
                missingParams.add(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG);
            if (false == missingParams.isEmpty())
                throw new ClientException(String.format("Missing required parameters: %s", missingParams.toString()));
            clientMode = new ValidateWorkloadMode(controlService);
        } else if (controlService.configuration().calculateWorkloadStatistics()) {
            // Calculate Statistics
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if (null == configuration.workloadClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
            if (0 == configuration.operationCount())
                missingParams.add(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG);
            if (false == missingParams.isEmpty())
                throw new ClientException(String.format("Missing required parameters: %s", missingParams.toString()));
            clientMode = new CalculateWorkloadStatisticsMode(controlService);
        } else {
            // Execute Workload
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if (null == configuration.dbClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.DB_ARG);
            if (null == configuration.workloadClassName())
                missingParams.add(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG);
            if (0 == configuration.operationCount())
                missingParams.add(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG);
            if (false == missingParams.isEmpty())
                throw new ClientException(String.format("Missing required parameters: %s", missingParams.toString()));
            clientMode = new ExecuteWorkloadMode(controlService, timeSource);
        }

        clientMode.init();
    }

    public void start() throws ClientException {
        clientMode.execute();
    }

    public DbValidationResult databaseValidationResult() {
        return databaseValidationResult;
    }

    public WorkloadValidationResult workloadValidationResult() {
        return workloadValidationResult;
    }

    public WorkloadStatistics workloadStatistics() {
        return workloadStatistics;
    }

    private interface ClientMode {
        void init() throws ClientException;

        void execute() throws ClientException;
    }

    private class ExecuteWorkloadMode implements ClientMode {
        private final TemporalUtil temporalUtil = new TemporalUtil();
        private final ConcurrentControlService controlService;
        private final TimeSource timeSource;

        private Workload workload = null;
        private Db database = null;
        private ConcurrentMetricsService metricsService = null;
        private ConcurrentCompletionTimeService completionTimeService = null;
        private WorkloadRunner workloadRunner = null;

        SimpleCsvFileWriter csvResultsLogFileWriter = null;

        ExecuteWorkloadMode(ConcurrentControlService controlService, TimeSource timeSource) throws ClientException {
            this.controlService = controlService;
            this.timeSource = timeSource;
        }

        @Override
        public void init() throws ClientException {
            if (null != controlService.configuration().resultDirPath()) {
                File resultDir = new File(controlService.configuration().resultDirPath());
                if (resultDir.exists() && false == resultDir.isDirectory())
                    throw new ClientException("Results directory is not directory: " + resultDir.getAbsolutePath());
                else if (false == resultDir.exists())
                    resultDir.mkdir();
            }

            CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();

            try {
                database = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                database.init(controlService.configuration().asMap());
            } catch (DbException e) {
                throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
            }
            logger.info(String.format("Loaded DB: %s", database.getClass().getName()));

            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

            try {
                completionTimeService =
                        completionTimeServiceAssistant.newThreadedQueuedConcurrentCompletionTimeServiceFromPeerIds(
                                timeSource,
                                controlService.configuration().peerIds(),
                                errorReporter
                        );
            } catch (CompletionTimeException e) {
                throw new ClientException(
                        String.format("Error while instantiating Completion Time Service with peer IDs %s", controlService.configuration().peerIds().toString()), e);
            }

            boolean recordStartTimeDelayLatency = false == controlService.configuration().ignoreScheduledStartTimes();
            ExecutionDelayPolicy executionDelayPolicy = new ErrorReportingTerminatingExecutionDelayPolicy(
                    timeSource,
                    controlService.configuration().toleratedExecutionDelayAsMilli(),
                    errorReporter);

            if (null != controlService.configuration().resultDirPath() && controlService.configuration().shouldCreateResultsLog()) {
                File resultDir = new File(controlService.configuration().resultDirPath());
                File resultsLog = new File(resultDir, controlService.configuration().name() + ThreadedQueuedConcurrentMetricsService.RESULTS_LOG_FILENAME_SUFFIX);
                try {
                    csvResultsLogFileWriter = new SimpleCsvFileWriter(resultsLog, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR);
                    csvResultsLogFileWriter.writeRow(
                            "operation_type",
                            "scheduled_start_time_ms",
                            "actual_start_time_ms",
                            "execution_duration_ns",
                            "result_code"
                    );
                } catch (IOException e) {
                    throw new ClientException(
                            String.format("Error while creating results log file: ", resultsLog.getAbsolutePath()), e);
                }
            }

            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                    timeSource,
                    errorReporter,
                    controlService.configuration().timeUnit(),
                    ThreadedQueuedConcurrentMetricsService.DEFAULT_HIGHEST_EXPECTED_RUNTIME_DURATION_AS_NANO,
                    recordStartTimeDelayLatency,
                    executionDelayPolicy,
                    csvResultsLogFileWriter);
            GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

            logger.info(String.format("Scanning workload streams to calculate their limits..."));
            WorkloadStreams workloadStreams;
            try {
                Tuple.Tuple2<WorkloadStreams, Workload> streamsAndWorkload =
                        WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(controlService.configuration(), gf);
                workload = streamsAndWorkload._2();
                workloadStreams = streamsAndWorkload._1();
            } catch (Exception e) {
                throw new ClientException(String.format("Error loading workload class: %s", controlService.configuration().workloadClassName()), e);
            }
            logger.info(String.format("Loaded workload: %s", workload.getClass().getName()));

            logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
            WorkloadStreams timeMappedWorkloadStreams;
            try {
                timeMappedWorkloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                        workloadStreams,
                        controlService.workloadStartTimeAsMilli(),
                        controlService.configuration().timeCompressionRatio(),
                        gf
                );
            } catch (WorkloadException e) {
                throw new ClientException("Error while retrieving operation stream for workload", e);
            }

            // TODO replace
            // TODO controlService.configuration().windowedExecutionWindowDuration().gt(controlService.configuration().compressedGctDeltaDuration())
            // TODO with
            // TODO controlService.configuration().windowedExecutionWindowDuration().gt(MIN_GAP_BETWEEN_DEPENDENT_OPERATIONS)
//            if (controlService.configuration().windowedExecutionWindowDuration().gt(controlService.configuration().compressedGctDeltaDuration()))
//                throw new ClientException(
//                        String.format(""
//                                + "Windowed-execution window duration may not exceed GCT delta duration\n"
//                                + "  GCT Delta: %s\n"
//                                + "  Compressed GCT Delta: %s\n"
//                                + "  Window Duration: %s",
//                                controlService.configuration().gctDeltaDuration(),
//                                controlService.configuration().compressedGctDeltaDuration(),
//                                controlService.configuration().windowedExecutionWindowDuration()));

            logger.info(String.format("Instantiating %s", WorkloadRunner.class.getSimpleName()));
            try {
                int operationHandlerExecutorsBoundedQueueSize = DefaultQueues.DEFAULT_BOUND_1000;
                workloadRunner = new WorkloadRunner(
                        timeSource,
                        database,
                        timeMappedWorkloadStreams,
                        metricsService,
                        errorReporter,
                        completionTimeService,
                        controlService.configuration().threadCount(),
                        controlService.configuration().statusDisplayIntervalAsSeconds(),
                        controlService.configuration().spinnerSleepDurationAsMilli(),
                        controlService.configuration().ignoreScheduledStartTimes(),
                        operationHandlerExecutorsBoundedQueueSize);
            } catch (WorkloadException e) {
                throw new ClientException(String.format("Error instantiating %s", WorkloadRunner.class.getSimpleName()), e);
            }

            logger.info("Initializing driver");
            try {
                if (completionTimeService.getAllWriters().isEmpty()) {
                    // There are no local completion time writers, GCT would never advance or be non-null, set to max so nothing ever waits on it
                    long nearlyMaxPossibleTimeAsMilli = Long.MAX_VALUE - 1;
                    long maxPossibleTimeAsMilli = Long.MAX_VALUE;
                    // Create a writer to use for advancing GCT
                    LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
                    localCompletionTimeWriter.submitLocalInitiatedTime(nearlyMaxPossibleTimeAsMilli);
                    localCompletionTimeWriter.submitLocalCompletedTime(nearlyMaxPossibleTimeAsMilli);
                    localCompletionTimeWriter.submitLocalInitiatedTime(maxPossibleTimeAsMilli);
                    localCompletionTimeWriter.submitLocalCompletedTime(maxPossibleTimeAsMilli);
                } else {
                    // There are some local completion time writers, initialize them to workload start time
                    completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, controlService.workloadStartTimeAsMilli() - 2);
                    completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, controlService.workloadStartTimeAsMilli() - 1);
                    completionTimeServiceAssistant.waitForGlobalCompletionTime(
                            timeSource,
                            controlService.workloadStartTimeAsMilli() - 1,
                            temporalUtil.convert(5, TimeUnit.SECONDS, TimeUnit.MILLISECONDS),
                            completionTimeService,
                            errorReporter
                    );
                    logger.info("GCT: " + TEMPORAL_UTIL.millisecondsToDateTimeString(completionTimeService.globalCompletionTimeAsMilli()));
                }
            } catch (CompletionTimeException e) {
                throw new ClientException("Error while writing initial initiated and completed times to Completion Time Service", e);
            }

            logger.info("Waiting for all driver processes to complete initialization");
            try {
                long globalCompletionTimeWaitTimeoutDurationAsMilli = temporalUtil.convert(5, TimeUnit.SECONDS, TimeUnit.MILLISECONDS);
                boolean globalCompletionTimeAdvancedToDesiredTime = completionTimeServiceAssistant.waitForGlobalCompletionTime(
                        timeSource,
                        controlService.workloadStartTimeAsMilli() - 2,
                        globalCompletionTimeWaitTimeoutDurationAsMilli,
                        completionTimeService,
                        errorReporter);
                if (false == globalCompletionTimeAdvancedToDesiredTime) {
                    throw new ClientException(
                            String.format("Timed out [%s] while waiting for global completion time to advance to workload start time\nCurrent GCT: %s\nWaiting For GCT: %s",
                                    globalCompletionTimeWaitTimeoutDurationAsMilli,
                                    completionTimeService.globalCompletionTimeAsMilli(),
                                    controlService.workloadStartTimeAsMilli())
                    );
                }
            } catch (CompletionTimeException e) {
                throw new ClientException(
                        String.format("Error encountered while waiting for global completion time to advance to workload start time: %s", controlService.workloadStartTimeAsMilli())
                );
            }
            logger.info("Initialization complete");

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            // TODO revise if this necessary here, and if not where??
            controlService.waitForCommandToExecuteWorkload();

            try (Workload w = workload; Db db = database) {
                workloadRunner.executeWorkload();
            } catch (WorkloadException e) {
                throw new ClientException("Error running Workload", e);
            } catch (IOException e) {
                throw new ClientException("Error running Workload", e);
            }

            // TODO revise if this necessary here, and if not where??
            controlService.waitForAllToCompleteExecutingWorkload();

            logger.info("Shutting down completion time service...");
            try {
                completionTimeService.shutdown();
            } catch (CompletionTimeException e) {
                throw new ClientException("Error during shutdown of completion time service", e);
            }

            logger.info("Shutting down metrics collection service...");
            WorkloadResultsSnapshot workloadResults;
            try {
                workloadResults = metricsService.results();
                metricsService.shutdown();
            } catch (MetricsCollectionException e) {
                throw new ClientException("Error during shutdown of metrics collection service", e);
            }

            logger.info("Exporting workload metrics...");
            try {
                MetricsManager.export(workloadResults, new SimpleOperationMetricsFormatter(), System.out, Charsets.UTF_8);
                if (null != controlService.configuration().resultDirPath()) {
                    File resultDir = new File(controlService.configuration().resultDirPath());
                    File resultFile = new File(resultDir, controlService.configuration().name() + ThreadedQueuedConcurrentMetricsService.RESULTS_METRICS_FILENAME_SUFFIX);
                    MetricsManager.export(workloadResults, new JsonOperationMetricsFormatter(), new FileOutputStream(resultFile), Charsets.UTF_8);

                    File configurationFile = new File(resultDir, controlService.configuration().name() + ThreadedQueuedConcurrentMetricsService.RESULTS_CONFIGURATION_FILENAME_SUFFIX);
                    try (PrintStream out = new PrintStream(new FileOutputStream(configurationFile))) {
                        out.print(controlService.configuration().toPropertiesString());
                    } catch (DriverConfigurationException e) {
                        throw new ClientException(
                                String.format("Encountered error while writing configuration to file.\nResult Dir: %s\nConfig File: %s",
                                        resultDir.getAbsolutePath(),
                                        configurationFile.getAbsolutePath()),
                                e
                        );
                    }
                    if (null != csvResultsLogFileWriter) {
                        csvResultsLogFileWriter.close();
                    }
                }
            } catch (MetricsCollectionException e) {
                throw new ClientException("Could not export workload metrics", e);
            } catch (FileNotFoundException e) {
                throw new ClientException("Error encountered while trying to write results", e);
            } catch (IOException e) {
                throw new ClientException("Error encountered while trying to write results", e);
            }
        }
    }

    private class CalculateWorkloadStatisticsMode implements ClientMode {
        private final ConcurrentControlService controlService;

        private Workload workload = null;
        private WorkloadStreams timeMappedWorkloadStreams = null;

        CalculateWorkloadStatisticsMode(ConcurrentControlService controlService) throws ClientException {
            this.controlService = controlService;
        }

        @Override
        public void init() throws ClientException {
            GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
            WorkloadStreams workloadStreams;
            try {
                Tuple.Tuple2<WorkloadStreams, Workload> workloadStreamsAndWorkload = WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(controlService.configuration(), gf);
                workloadStreams = workloadStreamsAndWorkload._1();
                workload = workloadStreamsAndWorkload._2();
            } catch (Exception e) {
                throw new ClientException(String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName()), e);
            }
            logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

            logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
            try {
                timeMappedWorkloadStreams = WorkloadStreams.timeOffsetAndCompressWorkloadStreams(
                        workloadStreams,
                        controlService.workloadStartTimeAsMilli(),
                        controlService.configuration().timeCompressionRatio(),
                        gf
                );
            } catch (WorkloadException e) {
                throw new ClientException("Error while retrieving operation stream for workload", e);
            }

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            TemporalUtil temporalUtil = new TemporalUtil();
            logger.info(String.format("Calculating workload statistics for: %s", workload.getClass().getSimpleName()));
            try (Workload w = workload) {
                WorkloadStatisticsCalculator workloadStatisticsCalculator = new WorkloadStatisticsCalculator();
                workloadStatistics = workloadStatisticsCalculator.calculate(
                        timeMappedWorkloadStreams,
                        temporalUtil.convert(5, TimeUnit.HOURS, TimeUnit.MILLISECONDS)
                        // TODO uncomment, maybe
                        // workload.maxExpectedInterleave()
                );
                logger.info("Calculation complete\n" + workloadStatistics);
            } catch (MetricsCollectionException e) {
                throw new ClientException("Error while calculating workload statistics", e);
            } catch (IOException e) {
                throw new ClientException("Error while calculating workload statistics", e);
            }
        }
    }

    private class CreateValidationParamsMode implements ClientMode {
        private final ConcurrentControlService controlService;

        private Workload workload = null;
        private Db database = null;
        private Iterator<Operation<?>> timeMappedOperations = null;

        CreateValidationParamsMode(ConcurrentControlService controlService) throws ClientException {
            this.controlService = controlService;
        }

        @Override
        public void init() throws ClientException {
            try {
                workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
                workload.init(controlService.configuration());
            } catch (Exception e) {
                throw new ClientException(String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName()), e);
            }
            logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

            try {
                database = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                database.init(controlService.configuration().asMap());
            } catch (DbException e) {
                throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
            }
            logger.info(String.format("Loaded DB: %s", database.getClass().getName()));

            GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

            logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
            try {
                Tuple.Tuple2<WorkloadStreams, Workload> streamsAndWorkload = WorkloadStreams.createNewWorkloadWithLimitedWorkloadStreams(controlService.configuration(), gf);
                workload = streamsAndWorkload._2();
                WorkloadStreams workloadStreams = streamsAndWorkload._1();
                timeMappedOperations = workloadStreams.mergeSortedByStartTime(gf);
            } catch (WorkloadException e) {
                throw new ClientException("Error while retrieving operation stream for workload", e);
            } catch (IOException e) {
                throw new ClientException("Error while retrieving operation stream for workload", e);
            }

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            try (Workload w = workload; Db db = database) {
                File validationFileToGenerate = new File(controlService.configuration().validationParamsCreationOptions().filePath());
                int validationSetSize = controlService.configuration().validationParamsCreationOptions().validationSetSize();
                // TODO get from config parameter
                boolean performSerializationMarshallingChecks = true;

                logger.info(String.format("Generating database validation file: %s", validationFileToGenerate.getAbsolutePath()));

                Iterator<ValidationParam> validationParamsGenerator =
                        new ValidationParamsGenerator(db, w.dbValidationParametersFilter(validationSetSize), timeMappedOperations);

                Iterator<String[]> csvRows =
                        new ValidationParamsToCsvRows(validationParamsGenerator, w, performSerializationMarshallingChecks);

                SimpleCsvFileWriter simpleCsvFileWriter;
                try {
                    simpleCsvFileWriter = new SimpleCsvFileWriter(validationFileToGenerate, SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR);
                } catch (IOException e) {
                    throw new ClientException("Error encountered trying to open CSV file writer", e);
                }

                try {
                    simpleCsvFileWriter.writeRows(csvRows);
                } catch (IOException e) {
                    throw new ClientException("Error encountered trying to write validation parameters to CSV file writer", e);
                }

                try {
                    simpleCsvFileWriter.close();
                } catch (IOException e) {
                    throw new ClientException("Error encountered trying to close CSV file writer", e);
                }

                int validationParametersGenerated = ((ValidationParamsGenerator) validationParamsGenerator).entriesWrittenSoFar();

                logger.info(String.format("Successfully generated %s database validation parameters", validationParametersGenerated));
            } catch (IOException e) {
                throw new ClientException("Error encountered duration validation parameter creation", e);
            }
        }
    }

    private class ValidateDatabaseMode implements ClientMode {
        private final ConcurrentControlService controlService;

        private Workload workload = null;
        private Db database = null;

        ValidateDatabaseMode(ConcurrentControlService controlService) throws ClientException {
            this.controlService = controlService;
        }

        @Override
        public void init() throws ClientException {
            try {
                workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
                workload.init(controlService.configuration());
            } catch (Exception e) {
                throw new ClientException(String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName()), e);
            }
            logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

            try {
                database = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                database.init(controlService.configuration().asMap());
            } catch (DbException e) {
                throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
            }
            logger.info(String.format("Loaded DB: %s", database.getClass().getName()));

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            try (Workload w = workload; Db db = database) {
                File validationParamsFile = new File(controlService.configuration().databaseValidationFilePath());

                logger.info(String.format("Validating database against expected results\n * Db: %s\n * Validation Params File: %s",
                        db.getClass().getName(), validationParamsFile.getAbsolutePath()));

                SimpleCsvFileReader validationParamsReader;
                try {
                    validationParamsReader = new SimpleCsvFileReader(validationParamsFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
                } catch (IOException e) {
                    throw new ClientException("Error encountered trying to create CSV file reader", e);
                }

                try {
                    Iterator<ValidationParam> validationParams = new ValidationParamsFromCsvRows(validationParamsReader, w);
                    DbValidator dbValidator = new DbValidator();
                    databaseValidationResult = dbValidator.validate(validationParams, db);
                    logger.info(databaseValidationResult.resultMessage());
                } catch (WorkloadException e) {
                    throw new ClientException(String.format("Error reading validation parameters file\nFile: %s", validationParamsFile.getAbsolutePath()), e);
                }
                validationParamsReader.close();

                File failedValidationOperationsFile = new File(validationParamsFile.getParentFile(), removeExtension(validationParamsFile.getName()) + "-failed-actual.json");
                try (PrintStream out = new PrintStream(new FileOutputStream(failedValidationOperationsFile))) {
                    out.print(databaseValidationResult.actualResultsForFailedOperationsAsJsonString(w));
                    out.flush();
                    out.close();
                } catch (Exception e) {
                    throw new ClientException(
                            String.format("Encountered error while writing to file\nFile: %s",
                                    failedValidationOperationsFile.getAbsolutePath()),
                            e
                    );
                }

                File expectedResultsForFailedValidationOperationsFile = new File(validationParamsFile.getParentFile(), removeExtension(validationParamsFile.getName()) + "-failed-expected.json");
                try (PrintStream out = new PrintStream(new FileOutputStream(expectedResultsForFailedValidationOperationsFile))) {
                    out.print(databaseValidationResult.expectedResultsForFailedOperationsAsJsonString(w));
                    out.flush();
                    out.close();
                } catch (Exception e) {
                    throw new ClientException(
                            String.format("Encountered error while writing to file\nFile: %s",
                                    failedValidationOperationsFile.getAbsolutePath()),
                            e
                    );
                }

                logger.info("Database Validation Successful");
            } catch (IOException e) {
                throw new ClientException("Error occurred during database validation", e);
            }
        }

        String removeExtension(String filename) {
            return (filename.indexOf(".") == -1) ? filename : filename.substring(0, filename.lastIndexOf("."));
        }

    }

    private class ValidateWorkloadMode implements ClientMode {
        private final ConcurrentControlService controlService;
        private final WorkloadFactory workloadFactory;

        ValidateWorkloadMode(final ConcurrentControlService controlService) throws ClientException {
            this.controlService = controlService;
            this.workloadFactory = new ClassNameWorkloadFactory(controlService.configuration().workloadClassName());
        }

        @Override
        public void init() throws ClientException {
            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            logger.info(String.format("Validating workload: %s", controlService.configuration().workloadClassName()));
            WorkloadValidator workloadValidator = new WorkloadValidator();
            workloadValidationResult = workloadValidator.validate(workloadFactory, controlService.configuration());
            if (workloadValidationResult.isSuccessful())
                logger.info("Workload Validation Result: PASS");
            else
                logger.info(String.format("Workload Validation Result: FAIL\n%s", workloadValidationResult.errorMessage()));
        }
    }

    private class PrintHelpMode implements ClientMode {
        private final ConcurrentControlService controlService;

        PrintHelpMode(ConcurrentControlService controlService) {
            this.controlService = controlService;
        }

        @Override
        public void init() throws ClientException {
        }

        @Override
        public void execute() throws ClientException {
            logger.info(controlService.configuration().helpString());
        }

    }
}