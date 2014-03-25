package com.ldbc.driver;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.WorkloadRunner;
import com.ldbc.driver.runtime.error.ConcurrentErrorReporter;
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

public class Client {
    private static Logger logger = Logger.getLogger(Client.class);

    private static final long RANDOM_SEED = 42;

    public static void main(String[] args) throws ClientException {
        try {
            WorkloadParams params = WorkloadParams.fromArgs(args);
            Client client = new Client(params);
            client.start();
        } catch (ParamsException e) {
            String errMsg = String.format("Error parsing parameters: %s", e.getMessage());
            logger.error(errMsg);
        } catch (Exception e) {
            logger.error("Client terminated unexpectedly", e);
        }
    }

    private final WorkloadParams params;
    private final Workload workload;
    private final Db db;
    private final ConcurrentErrorReporter errorReporter;
    private final ConcurrentMetricsService metricsService;
    private final GeneratorFactory generators;

    public Client(WorkloadParams params) throws ClientException {
        this.params = params;
        try {
            workload = ClassLoaderHelper.loadWorkload(params.workloadClassName());
            workload.init(params);
        } catch (Exception e) {
            String errMsg = String.format("Error loading Workload class: %s", params.workloadClassName());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        logger.info(String.format("Loaded Workload: %s", workload.getClass().getName()));

        try {
            db = ClassLoaderHelper.loadDb(params.dbClassName());
            db.init(params.asMap());
        } catch (DbException e) {
            String errMsg = String.format("Error loading DB class: %s", params.dbClassName());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        logger.info(String.format("Loaded DB: %s", db.getClass().getName()));

        errorReporter = new ConcurrentErrorReporter();
        metricsService = new ThreadedQueuedConcurrentMetricsService(errorReporter, params.timeUnit());
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    public void start() throws ClientException {
        logger.info("LDBC Workload Driver");
        logger.info(params.toString());

        // TODO get GCT DeltaT from configuration parameters
        Duration gctDeltaTime = Duration.fromMilli(0);

        WorkloadRunner workloadRunner;
        try {
            workloadRunner = new WorkloadRunner(
                    db,
                    workload.operations(generators),
                    workload.operationClassificationMapping(),
                    params.isShowStatus(),
                    params.threadCount(),
                    metricsService,
                    errorReporter,
                    gctDeltaTime);
        } catch (WorkloadException e) {
            String errMsg = "Error instantiating WorkloadRunner";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
        logger.info(String.format("Instantiated WorkloadRunner - Starting Benchmark (%s operations)", params.operationCount()));

        Time startTime = Time.now();
        try {
            workloadRunner.executeWorkload();
        } catch (WorkloadException e) {
            String errMsg = "Error running Workload";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
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
            if (null != params.resultFilePath()) {
                File resultFile = new File(params.resultFilePath());
                metricsService.export(new JsonOperationMetricsFormatter(), new FileOutputStream(resultFile));
            }
        } catch (MetricsCollectionException e) {
            String errMsg = "Could not export Measurements";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        } catch (FileNotFoundException e) {
            String errMsg = String.format("Error encountered while trying to write result file: %s", params.resultFilePath());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
    }
}