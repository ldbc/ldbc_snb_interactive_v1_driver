package com.ldbc.driver.modes;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.Db;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.generator.*;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.validation.*;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Iterator;

import static java.lang.String.format;

/**
 * This class is the driver mode CREATE_VALIDATION_PARAMS
 */
public class CreateValidationParamsMode extends DriverMode {

    private final ControlService controlService;
    private final LoggingService loggingService;
    private final long randomSeed;

    private Workload workload = null;
    private Db database = null;
    private Iterator<Operation> timeMappedOperations = null;

    public CreateValidationParamsMode(ControlService controlService, long randomSeed) {
        super(DriverModeType.CREATE_VALIDATION_PARAMS);
        this.controlService = controlService;
        this.loggingService = controlService.getLoggingServiceFactory().loggingServiceFor(getClass().getSimpleName());
        this.randomSeed = randomSeed;
    }

    /**
     * Initialize the workload (Interactive/BI), initialize the database connection and create operation streams.
     * @throws ClientException client exception
     */
    @Override
    public void init() throws ClientException {

        try {
            workload = ClassLoaderHelper.loadWorkload(controlService.getConfiguration().getWorkloadClassName());
            workload.init(controlService.getConfiguration());
        } catch (Exception e) {
            throw new ClientException(format("Error loading Workload class: %s",
                    controlService.getConfiguration().getWorkloadClassName()), e);
        }
        loggingService.info(format("Loaded Workload: %s", workload.getClass().getName()));

        try {
            database = ClassLoaderHelper.loadDb(controlService.getConfiguration().getDbClassName());
            database.init(
                    controlService.getConfiguration().asMap(),
                    controlService.getLoggingServiceFactory().loggingServiceFor(database.getClass().getSimpleName()),
                    workload.operationTypeToClassMapping()
            );
        } catch (Exception e) {
            throw new ClientException(
                    format("Error loading DB class: %s", controlService.getConfiguration().getDbClassName()), e);
        }
        loggingService.info(format("Loaded DB: %s", database.getClass().getName()));

        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(randomSeed));
        loggingService.info(
                format("Retrieving operation stream for workload: %s", workload.getClass().getSimpleName()));

        try {
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams, Workload, Long> streamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            controlService.getConfiguration(),
                            gf,
                            returnStreamsWithDbConnector,
                            0,
                            controlService.getConfiguration().getOperationCount(),
                            controlService.getLoggingServiceFactory()
                    );
            workload = streamsAndWorkload.getElement2();
            WorkloadStreams workloadStreams = streamsAndWorkload.getElement1();
            timeMappedOperations = WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators(gf, workloadStreams);
        } catch (Exception e) {
            throw new ClientException("Error while retrieving operation stream for workload", e);
        }

        loggingService.info("Driver Configuration");
        loggingService.info(controlService.toString());
    }

    /**
     * Execute operation streams, collect and results
     * @return Object used in tests
     * @throws ClientException client exception
     */
    @Override
    public Object startExecutionAndAwaitCompletion() throws ClientException {
        try (Workload w = workload; Db db = database) {
            File validationFileToGenerate =
                    new File(controlService.getConfiguration().getValidationParamsCreationOptions().getFilePath());
            int validationSetSize = controlService
                    .getConfiguration()
                    .getValidationParamsCreationOptions()
                    .getValidationSetSize();
            // TODO get from config parameter
            boolean performSerializationMarshallingChecks = true;

            loggingService.info(
                    format("Generating database validation file: %s", validationFileToGenerate.getAbsolutePath()));

            ParamsFilter paramsFilter = w.getValidationParamsFilter(validationSetSize);
            ValidationParamsGenerator validationParamsGenerator = new ValidationParamsGenerator(
                    db,
                    paramsFilter,
                    timeMappedOperations);

            Iterator<String[]> csvRows = new ValidationParamsToCsvRows(
                    validationParamsGenerator,
                    w,
                    performSerializationMarshallingChecks);

            int rowsWrittenSoFar = 0;
            try (SimpleCsvFileWriter simpleCsvFileWriter = new SimpleCsvFileWriter(
                    validationFileToGenerate,
                    SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR)) {
                DecimalFormat decimalFormat = new DecimalFormat("###,###,##0");
                while (csvRows.hasNext()) {
                    String[] csvRow = csvRows.next();
                    simpleCsvFileWriter.writeRow(csvRow);
                    rowsWrittenSoFar++;
                    if (rowsWrittenSoFar % 10 == 0) {
                        loggingService.info(
                                format(
                                        "%s / %s Validation Parameters Created\r",
                                        decimalFormat.format(rowsWrittenSoFar),
                                        decimalFormat.format(validationSetSize)
                                )
                        );
                    }
                }
            } catch (Exception e) {
                throw new ClientException("Error trying to write validation parameters to CSV file writer", e);
            }

            int validationParametersGenerated =
                    validationParamsGenerator.entriesWrittenSoFar();

            loggingService.info(format("Successfully generated %s database validation parameters",
                    validationParametersGenerated));
        } catch (Exception e) {
            throw new ClientException("Error encountered duration validation parameter creation", e);
        }
        return new Object();
    }


}
