package com.ldbc.driver;

import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.metrics.*;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.util.Iterator;

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);

    private static final long RANDOM_SEED = 42;

    public static void main(String[] args) throws ClientException {
        try {
            ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs(args);
            // TODO this method will not work with multiple processes - from controlService/config
            Time workloadStartTime = Time.now().plus(Duration.fromMilli(1000));
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, configuration);
            Client client = new Client(controlService);
            client.start();
        } catch (DriverConfigurationException e) {
            String errMsg = String.format("Error parsing parameters: %s", e.getMessage());
            logger.error(errMsg);
        } catch (Exception e) {
            logger.error("Client terminated unexpectedly", e);
        }
    }

    private final Workload workload;
    private final Db db;
    private final ConcurrentControlService controlService;
    private final ConcurrentMetricsService metricsService;
    private final WorkloadRunner workloadRunner;

    public Client(ConcurrentControlService controlService) throws ClientException {
        this.controlService = controlService;

        try {
            workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
            workload.init(controlService.configuration());
            // TODO add check that all ExecutionMode:GctMode combinations make sense (e.g., Partial+GctNone does not make sense unless window size can somehow be specified)
        } catch (Exception e) {
            throw new ClientException(
                    String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName()),
                    e.getCause());
        }
        logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

        try {
            db = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
            db.init(controlService.configuration().asMap());
        } catch (DbException e) {
            throw new ClientException(
                    String.format("Error loading DB class: %s", controlService.configuration().dbClassName()),
                    e.getCause());
        }
        logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

        ConcurrentErrorReporter errorReporter = new ConcurrentErrorReporter();
        metricsService = new ThreadedQueuedConcurrentMetricsService(errorReporter, controlService.configuration().timeUnit());
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));

        try {
            Iterator<Operation<?>> operations = workload.operations(generators);
            Iterator<Operation<?>> timeMappedOperations = generators.timeOffsetAndCompress(
                    operations,
                    controlService.workloadStartTime(),
                    controlService.configuration().timeCompressionRatio());

            workloadRunner = new WorkloadRunner(
                    controlService,
                    db,
                    timeMappedOperations,
                    workload.operationClassifications(),
                    metricsService,
                    errorReporter);
        } catch (WorkloadException e) {
            throw new ClientException("Error instantiating WorkloadRunner", e.getCause());
        }
        logger.info(String.format("Instantiated WorkloadRunner - Starting Benchmark (%s operations)", controlService.configuration().operationCount()));

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
            throw new ClientException(errMsg, e.getCause());
        }

        logger.info("Cleaning up DB...");
        try {
            db.cleanup();
        } catch (DbException e) {
            throw new ClientException("Error during DB cleanup", e.getCause());
        }

        logger.info("Shutting down metrics collection service...");
        WorkloadResults workloadResults;
        try {
            workloadResults = metricsService.results();
            metricsService.shutdown();
        } catch (MetricsCollectionException e) {
            throw new ClientException("Error during shutdown of metrics collection service", e.getCause());
        }

        logger.info(String.format("Runtime: %s (s)", workloadResults.finishTime().greaterBy(workloadResults.startTime()).asSeconds()));

        logger.info("Exporting workload metrics...");
        try {
            MetricsManager.export(workloadResults, new SimpleOperationMetricsFormatter(), System.out, MetricsManager.DEFAULT_CHARSET);
            if (null != controlService.configuration().resultFilePath()) {
                File resultFile = new File(controlService.configuration().resultFilePath());
                MetricsManager.export(workloadResults, new JsonOperationMetricsFormatter(), new FileOutputStream(resultFile), MetricsManager.DEFAULT_CHARSET);
            }
        } catch (MetricsCollectionException e) {
            throw new ClientException("Could not export workload metrics", e.getCause());
        } catch (FileNotFoundException e) {
            throw new ClientException(
                    String.format("Error encountered while trying to write result file: %s", controlService.configuration().resultFilePath()),
                    e.getCause());
        }
    }
}