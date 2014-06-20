package com.ldbc.driver;

import com.google.common.collect.Lists;
import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.coordination.CompletionTimeException;
import com.ldbc.driver.runtime.coordination.ConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.coordination.NaiveSynchronizedConcurrentCompletionTimeService;
import com.ldbc.driver.runtime.metrics.*;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.validation.DbValidator;
import com.ldbc.driver.validation.WorkloadStatistics;
import com.ldbc.driver.validation.WorkloadStatisticsCalculator;
import com.ldbc.driver.validation.WorkloadValidator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Future;

// TODO replace log4j with some interface like StatusReportingService

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);

    private static final long RANDOM_SEED = 42;

    public static void main(String[] args) throws ClientException {
        try {
            TimeSource systemTimeSource = new SystemTimeSource();
            ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(args);
            // TODO this method will not work with multiple processes - should come from controlService
            Time workloadStartTime = systemTimeSource.now().plus(Duration.fromSeconds(10));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
            Client client = new Client(controlService, systemTimeSource);
            client.start();
        } catch (DriverConfigurationException e) {
            String errMsg = String.format("Error parsing parameters: %s", e.getMessage());
            logger.error(errMsg);
        } catch (Exception e) {
            logger.error("Client terminated unexpectedly\n" + ConcurrentErrorReporter.stackTraceToString(e));
        }
    }

    private final Workload workload;
    private final Db db;
    private final ConcurrentControlService controlService;
    private final ConcurrentMetricsService metricsService;
    private final ConcurrentCompletionTimeService completionTimeService;
    private final WorkloadRunner workloadRunner;
    // This instance will be passed to ALL components and is the only way they measure time
    private final TimeSource TIME_SOURCE;

    // check results
    private boolean databaseLoadedCorrectly = false;
    private boolean workloadLoadedCorrectly = false;
    private boolean databaseValidationSuccessful = false;
    private WorkloadValidator.WorkloadValidationResult workloadValidationResult = null;
    private WorkloadStatistics workloadStatistics = null;

    public Client(ConcurrentControlService controlService, TimeSource timeSource) throws ClientException {
        TIME_SOURCE = timeSource;
        this.controlService = controlService;

        try {
            workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
            workload.init(controlService.configuration());
        } catch (Exception e) {
            throw new ClientException(String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName()), e);
        }
        workloadLoadedCorrectly = true;
        logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

        try {
            db = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
            db.init(controlService.configuration().asMap());
        } catch (DbException e) {
            throw new ClientException(String.format("Error loading DB class: %s", controlService.configuration().dbClassName()), e);
        }
        databaseLoadedCorrectly = true;
        logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();

        try {
            // TODO threaded may scale better with >8 cores, but consumes more resources & performs worse with <8 cores
//            completionTimeService = new ThreadedQueuedConcurrentCompletionTimeService(controlService.configuration().peerIds(), errorReporter);
            completionTimeService = new NaiveSynchronizedConcurrentCompletionTimeService(controlService.configuration().peerIds());
            completionTimeService.submitInitiatedTime(controlService.workloadStartTime());
            completionTimeService.submitCompletedTime(controlService.workloadStartTime());
            for (String peerId : controlService.configuration().peerIds()) {
                completionTimeService.submitExternalCompletionTime(peerId, controlService.workloadStartTime());
            }
            // Wait for workloadStartTime to be applied
            Future<Time> globalCompletionTimeFuture = completionTimeService.globalCompletionTimeFuture();
            while (false == globalCompletionTimeFuture.isDone()) {
                if (errorReporter.errorEncountered())
                    throw new WorkloadException(String.format("Encountered error while waiting for GCT to initialize. Driver terminating.\n%s", errorReporter.toString()));
            }
            if (false == globalCompletionTimeFuture.get().equals(controlService.workloadStartTime())) {
                throw new WorkloadException("Completion Time future failed to return expected value");
            }
        } catch (Exception e) {
            throw new ClientException(
                    String.format("Error while instantiating Completion Time Service with peer IDs %s", controlService.configuration().peerIds().toString()), e);
        }

        metricsService = new ThreadedQueuedConcurrentMetricsService(TIME_SOURCE, errorReporter, controlService.configuration().timeUnit());
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

        logger.info(String.format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));
        Iterator<Operation<?>> timeMappedOperations;
        try {
            Iterator<Operation<?>> operations = workload.operations(generators);
            timeMappedOperations = generators.timeOffsetAndCompress(
                    operations,
                    controlService.workloadStartTime(),
                    controlService.configuration().timeCompressionRatio());
        } catch (WorkloadException e) {
            throw new ClientException("Error while retrieving operation stream for workload", e);
        }

        if (controlService.configuration().validateDatabase()) {
            logger.info(String.format("Validating database: %s", db.getClass().getSimpleName()));
            try {
                DbValidator dbValidator = new DbValidator();
                Iterator<Tuple.Tuple2<Operation<?>, Object>> validationOperations = workload.validationOperations(generators);
                DbValidator.DbValidationResult dbValidationResult = dbValidator.validate(validationOperations, db);

                if (false == dbValidationResult.isSuccessful()) {
                    throw new ClientException(String.format("Database validation failed\n%s", dbValidationResult.errorMessage()));
                }
            } catch (WorkloadException e) {
                throw new ClientException(String.format("Encountered error while validating database implementation"), e);
            }
            databaseValidationSuccessful = true;
        }

        if (controlService.configuration().validateWorkload() || controlService.configuration().calculateWorkloadStatistics()) {
            List<Operation<?>> timeMappedOperationsList = Lists.newArrayList(timeMappedOperations);

            if (controlService.configuration().validateWorkload()) {
                logger.info(String.format("Validating workload: %s", workload.getClass().getSimpleName()));
                WorkloadValidator workloadValidator = new WorkloadValidator();
                workloadValidationResult = workloadValidator.validate(
                        timeMappedOperationsList.iterator(),
                        workload.operationClassifications(),
                        WorkloadValidator.DEFAULT_MAX_EXPECTED_INTERLEAVE);
                if (false == workloadValidationResult.isSuccessful()) {
                    throw new ClientException(String.format("Workload validation failed\n%s", workloadValidationResult.errorMessage()));
                }
            }

            if (controlService.configuration().calculateWorkloadStatistics()) {
                logger.info(String.format("Calculating workload statistics for: %s", workload.getClass().getSimpleName()));
                try {
                    WorkloadStatisticsCalculator workloadStatisticsCalculator = new WorkloadStatisticsCalculator();
                    workloadStatistics = workloadStatisticsCalculator.calculate(
                            timeMappedOperationsList.iterator(),
                            workload.operationClassifications(),
                            WorkloadValidator.DEFAULT_MAX_EXPECTED_INTERLEAVE);
                    logger.info("Calculation complete\n" + workloadStatistics);
                } catch (MetricsCollectionException e) {
                    throw new ClientException("Error while calculating workload statistics", e);
                }
            }

            timeMappedOperations = timeMappedOperationsList.iterator();
        }

        logger.info(String.format("Instantiating %s", WorkloadRunner.class.getSimpleName()));
        try {
            workloadRunner = new WorkloadRunner(
                    TIME_SOURCE,
                    controlService,
                    db,
                    timeMappedOperations,
                    workload.operationClassifications(),
                    metricsService,
                    errorReporter,
                    completionTimeService);
        } catch (WorkloadException e) {
            throw new ClientException(String.format("Error instantiating %s", WorkloadRunner.class.getSimpleName()), e);
        }
        logger.info(String.format("Instantiated %s - Starting Benchmark (%s operations)", WorkloadRunner.class.getSimpleName(), controlService.configuration().operationCount()));

        logger.info("LDBC Workload Driver");
        logger.info(controlService.toString());
    }

    public void start() throws ClientException {
        try {
            workloadRunner.executeWorkload();
        } catch (WorkloadException e) {
            throw new ClientException("Error running Workload", e);
        }

        logger.info("Cleaning up Workload...");
        try {
            workload.cleanup();
        } catch (WorkloadException e) {
            String errMsg = "Error during Workload cleanup";
            throw new ClientException(errMsg, e);
        }

        logger.info("Cleaning up DB...");
        try {
            db.cleanup();
        } catch (DbException e) {
            throw new ClientException("Error during DB cleanup", e);
        }

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
            controlService.shutdown();
        } catch (MetricsCollectionException e) {
            throw new ClientException("Could not export workload metrics", e);
        } catch (FileNotFoundException e) {
            throw new ClientException(
                    String.format("Error encountered while trying to write result file: %s", controlService.configuration().resultFilePath()), e);
        }
    }

    public boolean databaseLoadedCorrectly() {
        return databaseLoadedCorrectly;
    }

    public boolean workloadLoadedCorrectly() {
        return workloadLoadedCorrectly;
    }

    public boolean databaseValidationResult() {
        return databaseValidationSuccessful;
    }

    public WorkloadValidator.WorkloadValidationResult workloadValidationResult() {
        return workloadValidationResult;
    }

    public WorkloadStatistics workloadStatistics() {
        return workloadStatistics;
    }
}