package com.ldbc.driver;

import com.ldbc.driver.control.ConcurrentControlService;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.ParamsException;
import com.ldbc.driver.control.WorkloadParams;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.metrics.ConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.MetricsCollectionException;
import com.ldbc.driver.runtime.metrics.ThreadedQueuedConcurrentMetricsService;
import com.ldbc.driver.runtime.metrics.formatters.JsonOperationMetricsFormatter;
import com.ldbc.driver.runtime.metrics.formatters.SimpleOperationMetricsFormatter;
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
            WorkloadParams params = WorkloadParams.fromArgs(args);
            // TODO this method will not work with multiple processes - from controlService/config
            Time workloadStartTime = Time.now().plus(Duration.fromMilli(1000));
            // TODO create real control service (LocalControlService(config) & DistributedControlService(address)?)
            ConcurrentControlService controlService = new LocalControlService(workloadStartTime, params);
            Client client = new Client(controlService);
            client.start();
        } catch (ParamsException e) {
            String errMsg = String.format("Error parsing parameters: %s", e.getMessage());
            logger.error(errMsg);
        } catch (Exception e) {
            logger.error("Client terminated unexpectedly", e);
        }
    }

    private final Workload workload;
    private final Db db;
    private final ConcurrentControlService controlService;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentMetricsService metricsService;
    private final GeneratorFactory generators;

    public Client(ConcurrentControlService controlService) throws ClientException {
        // TODO create real control service (LocalControlService(config) & DistributedControlService(address)?)
        // TODO this method will not work with multiple processes
        // TODO get all of these from controlService/config
        this.controlService = controlService;

        try {
            workload = ClassLoaderHelper.loadWorkload(controlService.configuration().workloadClassName());
            workload.init(controlService.configuration());
            // TODO add check that all ExecutionMode:GctMode combinations make sense (e.g., Partial+GctNone does not make sense unless window size can somehow be specified)
        } catch (Exception e) {
            String errMsg = String.format("Error loading Workload class: %s", controlService.configuration().workloadClassName());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

        try {
            db = ClassLoaderHelper.loadDb(controlService.configuration().dbClassName());
            db.init(controlService.configuration().asMap());
        } catch (DbException e) {
            String errMsg = String.format("Error loading DB class: %s", controlService.configuration().dbClassName());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

        errorReporter = new ConcurrentErrorReporter();
        // TODO set metrics start time somehow. may need to do this in start(). metrics service will need support for this in its interface.
        metricsService = new ThreadedQueuedConcurrentMetricsService(errorReporter, controlService.configuration().timeUnit());
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    public void start() throws ClientException {
        logger.info("LDBC Workload Driver");
        logger.info(controlService.toString());

        WorkloadRunner workloadRunner;
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
                    workload.operationClassificationMapping(),
                    metricsService,
                    errorReporter);
        } catch (WorkloadException e) {
            String errMsg = "Error instantiating WorkloadRunner";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        logger.info(String.format("Instantiated WorkloadRunner - Starting Benchmark (%s operations)", controlService.configuration().operationCount()));

        // TODO this should be taken care of by MetricsService
        Time startTime = Time.now();
        try {
            workloadRunner.executeWorkload();
        } catch (WorkloadException e) {
            String errMsg = "Error running Workload";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        // TODO this should be taken care of by MetricsService
        Time endTime = Time.now();

        logger.info("Cleaning up Workload...");
        try {
            workload.cleanup();
        } catch (WorkloadException e) {
            String errMsg = "Error during Workload cleanup";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }

        logger.info("Cleaning up DB...");
        try {
            db.cleanup();
        } catch (DbException e) {
            String errMsg = "Error during DB cleanup";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }

        logger.info(String.format("Runtime: %s (s)", endTime.greaterBy(startTime).asSeconds()));
        logger.info("Exporting Measurements...");
        try {
            metricsService.export(new SimpleOperationMetricsFormatter(), System.out);
            if (null != controlService.configuration().resultFilePath()) {
                File resultFile = new File(controlService.configuration().resultFilePath());
                metricsService.export(new JsonOperationMetricsFormatter(), new FileOutputStream(resultFile));
            }
        } catch (MetricsCollectionException e) {
            String errMsg = "Could not export Measurements";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        } catch (FileNotFoundException e) {
            String errMsg = String.format("Error encountered while trying to write result file: %s", controlService.configuration().resultFilePath());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
    }
}