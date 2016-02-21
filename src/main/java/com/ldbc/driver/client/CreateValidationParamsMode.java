package com.ldbc.driver.client;

import com.ldbc.driver.ClientException;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.WorkloadStreams;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.csv.simple.SimpleCsvFileWriter;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.Tuple3;
import com.ldbc.driver.validation.ValidationParam;
import com.ldbc.driver.validation.ValidationParamsGenerator;
import com.ldbc.driver.validation.ValidationParamsToCsvRows;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import static java.lang.String.format;

public class CreateValidationParamsMode implements ClientMode<Object>
{
    private final ControlService controlService;
    private final LoggingService loggingService;
    private final long randomSeed;

    private Workload workload = null;
    private Db database = null;
    private Iterator<Operation> timeMappedOperations = null;

    public CreateValidationParamsMode( ControlService controlService, long randomSeed ) throws ClientException
    {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
        this.randomSeed = randomSeed;
    }

    @Override
    public void init() throws ClientException
    {
        try
        {
            workload = ClassLoaderHelper.loadWorkload( controlService.configuration().workloadClassName() );
            workload.init( controlService.configuration() );
        }
        catch ( Exception e )
        {
            throw new ClientException( format( "Error loading Workload class: %s",
                    controlService.configuration().workloadClassName() ), e );
        }
        loggingService.info( format( "Loaded Workload: %s", workload.getClass().getName() ) );

        try
        {
            database = ClassLoaderHelper.loadDb( controlService.configuration().dbClassName() );
            database.init(
                    controlService.configuration().asMap(),
                    controlService.loggingServiceFactory().loggingServiceFor( database.getClass().getSimpleName() ),
                    workload.operationTypeToClassMapping()
            );
        }
        catch ( Exception e )
        {
            throw new ClientException(
                    format( "Error loading DB class: %s", controlService.configuration().dbClassName() ), e );
        }
        loggingService.info( format( "Loaded DB: %s", database.getClass().getName() ) );

        GeneratorFactory gf = new GeneratorFactory( new RandomDataGeneratorFactory( randomSeed ) );

        loggingService.info(
                format( "Retrieving operation stream for workload: %s", workload.getClass().getSimpleName() ) );
        try
        {
            boolean returnStreamsWithDbConnector = false;
            Tuple3<WorkloadStreams,Workload,Long> streamsAndWorkload =
                    WorkloadStreams.createNewWorkloadWithOffsetAndLimitedWorkloadStreams(
                            controlService.configuration(),
                            gf,
                            returnStreamsWithDbConnector,
                            0,
                            controlService.configuration().operationCount(),
                            controlService.loggingServiceFactory()
                    );
            workload = streamsAndWorkload._2();
            WorkloadStreams workloadStreams = streamsAndWorkload._1();
            timeMappedOperations =
                    WorkloadStreams.mergeSortedByStartTimeExcludingChildOperationGenerators( gf, workloadStreams );
        }
        catch ( Exception e )
        {
            throw new ClientException( "Error while retrieving operation stream for workload", e );
        }

        loggingService.info( "Driver Configuration" );
        loggingService.info( controlService.toString() );
    }

    @Override
    public Object startExecutionAndAwaitCompletion() throws ClientException
    {
        try ( Workload w = workload; Db db = database )
        {
            File validationFileToGenerate =
                    new File( controlService.configuration().validationParamsCreationOptions().filePath() );
            int validationSetSize = controlService
                    .configuration()
                    .validationParamsCreationOptions()
                    .validationSetSize();
            // TODO get from config parameter
            boolean performSerializationMarshallingChecks = true;

            loggingService.info(
                    format( "Generating database validation file: %s", validationFileToGenerate.getAbsolutePath() ) );

            Iterator<ValidationParam> validationParamsGenerator = new ValidationParamsGenerator(
                    db,
                    w.dbValidationParametersFilter( validationSetSize ),
                    timeMappedOperations );

            Iterator<String[]> csvRows = new ValidationParamsToCsvRows(
                    validationParamsGenerator,
                    w,
                    performSerializationMarshallingChecks );

            int rowsWrittenSoFar = 0;
            try ( SimpleCsvFileWriter simpleCsvFileWriter = new SimpleCsvFileWriter(
                    validationFileToGenerate,
                    SimpleCsvFileWriter.DEFAULT_COLUMN_SEPARATOR ) )
            {
                DecimalFormat decimalFormat = new DecimalFormat( "###,###,##0" );
                while ( csvRows.hasNext() )
                {
                    String[] csvRow = csvRows.next();
                    simpleCsvFileWriter.writeRow( csvRow );
                    rowsWrittenSoFar++;
                    if ( rowsWrittenSoFar % 10 == 0 )
                    {
                        loggingService.info(
                                format(
                                        "%s / %s Validation Parameters Created\r",
                                        decimalFormat.format( rowsWrittenSoFar ),
                                        decimalFormat.format( validationSetSize )
                                )
                        );
                    }
                }
            }
            catch ( Exception e )
            {
                throw new ClientException( "Error trying to write validation parameters to CSV file writer", e );
            }

            int validationParametersGenerated =
                    ((ValidationParamsGenerator) validationParamsGenerator).entriesWrittenSoFar();

            loggingService.info( format( "Successfully generated %s database validation parameters",
                    validationParametersGenerated ) );
        }
        catch ( Exception e )
        {
            throw new ClientException( "Error encountered duration validation parameter creation", e );
        }
        return null;
    }
}
