package com.ldbc.driver;

import com.google.common.collect.Lists;
import com.ldbc.driver.control.*;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.CompletionTimeServiceAssistant;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.LocalCompletionTimeWriter;
import com.ldbc.driver.runtime.metrics.*;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.CsvFileWriter;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.validation.*;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// TODO replace log4j with some interface like StatusReportingService

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);

    private static final long RANDOM_SEED = 42;

    public static void main(String[] args) throws ClientException {
        ConcurrentControlService controlService = null;
        try {
            TimeSource systemTimeSource = new SystemTimeSource();
            ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(args);
            // TODO this method will not work with multiple processes - should come from controlService
            Time workloadStartTime = systemTimeSource.now().plus(Duration.fromSeconds(10));
            controlService = new LocalControlService(workloadStartTime, configuration);
            Client client = new Client(controlService, systemTimeSource);
            client.start();
        } catch (DriverConfigurationException e) {
            String errMsg = String.format("Error parsing parameters: %s", e.getMessage());
            logger.error(errMsg);
        } catch (Exception e) {
            logger.error("Client terminated unexpectedly\n" + ConcurrentErrorReporter.stackTraceToString(e));
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
        private final ConcurrentControlService controlService;
        private final TimeSource timeSource;

        private Workload workload = null;
        private Db db = null;
        private ConcurrentMetricsService metricsService = null;
        private ConcurrentCompletionTimeService completionTimeService = null;
        private WorkloadRunner workloadRunner = null;

        ExecuteWorkloadMode(ConcurrentControlService controlService, TimeSource timeSource) throws ClientException {
            this.controlService = controlService;
            this.timeSource = timeSource;
        }

        @Override
        public void init() throws ClientException {
            CompletionTimeServiceAssistant completionTimeServiceAssistant = new CompletionTimeServiceAssistant();
            try {
                workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
                workload.init(controlService.configuration());
            } catch (Exception e) {
                throw new ClientException(String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName()), e);
            }
            logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

            try {
                db = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                db.init(controlService.configuration().asMap());
            } catch (DbException e) {
                throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
            }
            logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

            ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

            try {
                completionTimeService =
                        completionTimeServiceAssistant.newSynchronizedConcurrentCompletionTimeServiceFromPeerIds(
                                controlService.configuration().peerIds());
            } catch (CompletionTimeException e) {
                throw new ClientException(
                        String.format("Error while instantiating Completion Time Service with peer IDs %s", controlService.configuration().peerIds().toString()), e);
            }

            metricsService = ThreadedQueuedConcurrentMetricsService.newInstanceUsingBlockingQueue(
                    timeSource,
                    errorReporter,
                    controlService.configuration().timeUnit(),
                    controlService.workloadStartTime());
            GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

            logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
            Iterator<Operation<?>> timeMappedOperations;
            try {
                Iterator<Operation<?>> operations = workload.operations(generators, controlService.configuration().operationCount());
                timeMappedOperations = generators.timeOffsetAndCompress(
                        operations,
                        controlService.workloadStartTime(),
                        controlService.configuration().timeCompressionRatio());
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
                // TODO consider making config parameter
                Duration earlySpinnerOffsetDuration = WorkloadRunner.EARLY_SPINNER_OFFSET_DURATION;
                workloadRunner = new WorkloadRunner(
                        timeSource,
                        db,
                        timeMappedOperations,
                        workload.getOperationClassifications(),
                        metricsService,
                        errorReporter,
                        completionTimeService,
                        controlService.configuration().threadCount(),
                        controlService.configuration().statusDisplayInterval(),
                        controlService.workloadStartTime(),
                        controlService.configuration().toleratedExecutionDelay(),
                        controlService.configuration().spinnerSleepDuration(),
                        controlService.configuration().windowedExecutionWindowDuration(),
                        earlySpinnerOffsetDuration
                );
            } catch (WorkloadException e) {
                throw new ClientException(String.format("Error instantiating %s", WorkloadRunner.class.getSimpleName()), e);
            }
            logger.info(String.format("Instantiated %s - Operation Count = %s", WorkloadRunner.class.getSimpleName(), controlService.configuration().operationCount()));

            logger.info("Initializing driver");
            try {
                if (completionTimeService.getAllWriters().isEmpty()) {
                    // There are no local completion time writers, GCT would never advance or be non-null, set to max so nothing ever waits on it
                    Time nearlyMaxPossibleTime = Time.fromNano(Long.MAX_VALUE - 1);
                    Time maxPossibleTime = Time.fromNano(Long.MAX_VALUE);
                    // Create a writer to use for advancing GCT
                    LocalCompletionTimeWriter localCompletionTimeWriter = completionTimeService.newLocalCompletionTimeWriter();
                    localCompletionTimeWriter.submitLocalInitiatedTime(nearlyMaxPossibleTime);
                    localCompletionTimeWriter.submitLocalCompletedTime(nearlyMaxPossibleTime);
                    localCompletionTimeWriter.submitLocalInitiatedTime(maxPossibleTime);
                    localCompletionTimeWriter.submitLocalCompletedTime(maxPossibleTime);
                } else {
                    // There are some local completion time writers, initialize them to workload start time
                    completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, controlService.workloadStartTime());
                    completionTimeServiceAssistant.writeInitiatedAndCompletedTimesToAllWriters(completionTimeService, controlService.workloadStartTime().plus(Duration.fromNano(1)));
                }
            } catch (CompletionTimeException e) {
                throw new ClientException("Error while writing initial initiated and completed times to Completion Time Service", e);
            }

            logger.info("Waiting for all driver processes to complete initialization");
            try {
                Duration globalCompletionTimeWaitTimeoutDuration = Duration.fromSeconds(5);
                boolean globalCompletionTimeAdvancedToDesiredTime = completionTimeServiceAssistant.waitForGlobalCompletionTime(
                        timeSource,
                        controlService.workloadStartTime(),
                        globalCompletionTimeWaitTimeoutDuration,
                        completionTimeService,
                        errorReporter);
                if (false == globalCompletionTimeAdvancedToDesiredTime) {
                    throw new ClientException(
                            String.format("Timed out [%s] while waiting for global completion time to advance to workload start time\nCurrent GCT: %s\nWaiting For GCT: %s",
                                    globalCompletionTimeWaitTimeoutDuration,
                                    completionTimeService.globalCompletionTime(),
                                    controlService.workloadStartTime())
                    );
                }
            } catch (CompletionTimeException e) {
                throw new ClientException(
                        String.format("Error encountered while waiting for global completion time to advance to workload start time: %s", controlService.workloadStartTime())
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

            try {
                workloadRunner.executeWorkload();
            } catch (WorkloadException e) {
                throw new ClientException("Error running Workload", e);
            }

            // TODO revise if this necessary here, and if not where??
            controlService.waitForAllToCompleteExecutingWorkload();

            cleanupWorkload(workload);
            cleanupDb(db);

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

            logger.info(String.format("Runtime: %s", workloadResults.totalRunDuration()));

            logger.info("Exporting workload metrics...");
            try {
                MetricsManager.export(workloadResults, new SimpleOperationMetricsFormatter(), System.out, MetricsManager.DEFAULT_CHARSET);
                if (null != controlService.configuration().resultFilePath()) {
                    File resultFile = new File(controlService.configuration().resultFilePath());
                    MetricsManager.export(workloadResults, new JsonOperationMetricsFormatter(), new FileOutputStream(resultFile), MetricsManager.DEFAULT_CHARSET);
                }
            } catch (MetricsCollectionException e) {
                throw new ClientException("Could not export workload metrics", e);
            } catch (FileNotFoundException e) {
                throw new ClientException(
                        String.format("Error encountered while trying to write result file: %s", controlService.configuration().resultFilePath()), e);
            }
        }
    }

    private class CalculateWorkloadStatisticsMode implements ClientMode {
        private final ConcurrentControlService controlService;

        private Workload workload = null;
        private Iterator<Operation<?>> timeMappedOperations = null;

        CalculateWorkloadStatisticsMode(ConcurrentControlService controlService) throws ClientException {
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

            GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

            logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
            try {
                Iterator<Operation<?>> operations = workload.operations(generators, controlService.configuration().operationCount());
                timeMappedOperations = generators.timeOffsetAndCompress(
                        operations,
                        controlService.workloadStartTime(),
                        controlService.configuration().timeCompressionRatio());
            } catch (WorkloadException e) {
                throw new ClientException("Error while retrieving operation stream for workload", e);
            }

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            logger.info(String.format("Calculating workload statistics for: %s", workload.getClass().getSimpleName()));
            try {
                WorkloadStatisticsCalculator workloadStatisticsCalculator = new WorkloadStatisticsCalculator();
                workloadStatistics = workloadStatisticsCalculator.calculate(
                        timeMappedOperations,
                        workload.getOperationClassifications(),
                        workload.maxExpectedInterleave());
                logger.info("Calculation complete\n" + workloadStatistics);
            } catch (MetricsCollectionException e) {
                throw new ClientException("Error while calculating workload statistics", e);
            }

            cleanupWorkload(workload);
        }
    }

    private class CreateValidationParamsMode implements ClientMode {
        private final ConcurrentControlService controlService;

        private Workload workload = null;
        private Db db = null;
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
                db = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                db.init(controlService.configuration().asMap());
            } catch (DbException e) {
                throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
            }
            logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

            GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

            logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
            try {
                Iterator<Operation<?>> operations = workload.operations(generators, controlService.configuration().operationCount());
                timeMappedOperations = generators.timeOffsetAndCompress(
                        operations,
                        controlService.workloadStartTime(),
                        controlService.configuration().timeCompressionRatio());
            } catch (WorkloadException e) {
                throw new ClientException("Error while retrieving operation stream for workload", e);
            }

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            File validationFileToGenerate = new File(controlService.configuration().validationParamsCreationOptions().filePath());
            int validationSetSize = controlService.configuration().validationParamsCreationOptions().validationSetSize();
            // TODO get from config parameter
            boolean performSerializationMarshallingChecks = true;

            logger.info(String.format("Generating database validation file: %s", validationFileToGenerate.getAbsolutePath()));

            List<Operation<?>> timeMappedOperationsList = Lists.newArrayList(timeMappedOperations);

            Iterator<ValidationParam> validationParamsGenerator =
                    new ValidationParamsGenerator(db, workload.dbValidationParametersFilter(validationSetSize), timeMappedOperationsList.iterator());

            Iterator<String[]> csvRows =
                    new ValidationParamsToCsvRows(validationParamsGenerator, workload, performSerializationMarshallingChecks);

            CsvFileWriter csvFileWriter;
            try {
                csvFileWriter = new CsvFileWriter(validationFileToGenerate, CsvFileWriter.DEFAULT_COLUMN_SEPARATOR_STRING);
            } catch (IOException e) {
                throw new ClientException("Error encountered trying to open CSV file writer", e);
            }

            try {
                csvFileWriter.writeRows(csvRows);
            } catch (IOException e) {
                throw new ClientException("Error encountered trying to write validation parameters to CSV file writer", e);
            }

            try {
                csvFileWriter.close();
            } catch (IOException e) {
                throw new ClientException("Error encountered trying to close CSV file writer", e);
            }

            int validationParametersGenerated = ((ValidationParamsGenerator) validationParamsGenerator).entriesWrittenSoFar();

            timeMappedOperations = timeMappedOperationsList.iterator();
            logger.info(String.format("Successfully generated %s database validation parameters", validationParametersGenerated));

            cleanupWorkload(workload);
            cleanupDb(db);
        }
    }

    private class ValidateDatabaseMode implements ClientMode {
        private final ConcurrentControlService controlService;

        private Workload workload = null;
        private Db db = null;

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
                db = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
                db.init(controlService.configuration().asMap());
            } catch (DbException e) {
                throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
            }
            logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

            logger.info("Driver Configuration");
            logger.info(controlService.toString());
        }

        @Override
        public void execute() throws ClientException {
            File validationParamsFile = new File(controlService.configuration().databaseValidationFilePath());

            logger.info(String.format("Validating database against expected results\n * Db: %s\n * Validation Params File: %s",
                    db.getClass().getName(), validationParamsFile.getAbsolutePath()));

            CsvFileReader csvFileReader;
            try {
                csvFileReader = new CsvFileReader(validationParamsFile, CsvFileWriter.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING);
            } catch (IOException e) {
                throw new ClientException("Error encountered trying to create CSV file reader", e);
            }

            try {
                Iterator<ValidationParam> validationParams = new ValidationParamsFromCsvRows(csvFileReader, workload);
                DbValidator dbValidator = new DbValidator();
                databaseValidationResult = dbValidator.validate(validationParams, db);
                logger.info(databaseValidationResult.resultMessage());
            } catch (WorkloadException e) {
                throw new ClientException(String.format("Error reading validation parameters file\nFile: %s", validationParamsFile.getAbsolutePath()), e);
            }

            csvFileReader.closeReader();

            logger.info("Database Validation Successful");

            cleanupWorkload(workload);
            cleanupDb(db);
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

    private final void cleanupDb(Db db) throws ClientException {
        logger.info("Cleaning up DB...");
        try {
            db.cleanup();
        } catch (DbException e) {
            throw new ClientException("Error during DB cleanup", e);
        }
        logger.info("Complete");
    }

    private final void cleanupWorkload(Workload workload) throws ClientException {
        logger.info("Cleaning up Workload...");
        try {
            workload.cleanup();
        } catch (WorkloadException e) {
            String errMsg = "Error during Workload cleanup";
            throw new ClientException(errMsg, e);
        }
        logger.info("Complete");
    }
}