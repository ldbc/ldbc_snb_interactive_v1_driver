package com.ldbc.driver;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.metrics.WorkloadMetricsManager;
import com.ldbc.driver.metrics.formatters.JsonOperationMetricsFormatter;
import com.ldbc.driver.metrics.formatters.SimpleOperationMetricsFormatter;
import com.ldbc.driver.runner.WorkloadRunner;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.temporal.Time;
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
    private final WorkloadMetricsManager metricsManager;
    private final GeneratorFactory generators;

    public WorkloadParams params() {
        return params;
    }

//    public Workload workload() {
//        return workload;
//    }
//
//    public Db db() {
//        return db;
//    }

    public GeneratorFactory generators() {
        return generators;
    }

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

        metricsManager = new WorkloadMetricsManager(params.timeUnit());

        generators = new GeneratorFactory(new RandomDataGeneratorFactory(RANDOM_SEED));
    }

    public void start() throws ClientException {
        logger.info("LDBC Workload Driver");
        logger.info(params.toString());

        WorkloadRunner workloadRunner = null;
        try {
            Iterator<Operation<?>> operationGenerator = getOperationGenerator(workload, params.benchmarkPhase(),
                    generators);
            workloadRunner = new WorkloadRunner(db, operationGenerator, params.isShowStatus(),
                    params.threadCount(), metricsManager);
        } catch (WorkloadException e) {
            String errMsg = "Error instantiating WorkloadRunner";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }

        logger.info(String.format("Starting Benchmark (%s operations)", params.operationCount()));

        Time startTime = Time.now();
        try {
            workloadRunner.run();
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
            metricsManager.export(new SimpleOperationMetricsFormatter(), System.out);
            if (null != params.resultFilePath()) {
                File resultFile = new File(params.resultFilePath());
                metricsManager.export(new JsonOperationMetricsFormatter(), new FileOutputStream(resultFile));
            }
        } catch (WorkloadException e) {
            String errMsg = "Could not export Measurements";
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        } catch (FileNotFoundException e) {
            String errMsg = String.format("Error encountered while trying to write result file: ", params.resultFilePath());
            logger.error(errMsg, e);
            throw new ClientException(errMsg, e.getCause());
        }
    }

    private Iterator<Operation<?>> getOperationGenerator(Workload workload, BenchmarkPhase benchmarkPhase,
                                                         GeneratorFactory generators) throws WorkloadException {
        switch (benchmarkPhase) {
            case LOAD_PHASE:
                return workload.getLoadOperations(generators);
            case TRANSACTION_PHASE:
                return workload.getTransactionalOperations(generators);
        }
        throw new WorkloadException("Error encountered trying to get operation generator");
    }
}