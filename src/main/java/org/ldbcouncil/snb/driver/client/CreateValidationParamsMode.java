package org.ldbcouncil.snb.driver.client;
/**
 * CreateValidationParamsMode.java
 * 
 * Create class to generate validation queries with their results and write them to disk.
 * 
 */

import org.ldbcouncil.snb.driver.ClientException;
import org.ldbcouncil.snb.driver.Db;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.WorkloadStreams;
import org.ldbcouncil.snb.driver.control.ControlService;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.generator.GeneratorFactory;
import org.ldbcouncil.snb.driver.generator.RandomDataGeneratorFactory;
import org.ldbcouncil.snb.driver.util.ClassLoaderHelper;
import org.ldbcouncil.snb.driver.util.Tuple3;
import org.ldbcouncil.snb.driver.validation.ValidationParam;
import org.ldbcouncil.snb.driver.validation.ValidationParamsGenerator;
import org.ldbcouncil.snb.driver.validation.ValidationParamsToJson;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

public class CreateValidationParamsMode implements ClientMode<Object>
{
    private final ControlService controlService;
    private final LoggingService loggingService;
    private final long randomSeed;

    private Workload workload = null;
    private Db database = null;
    private Iterator<Operation> timeMappedOperations = null;

    /**
     * Create class to generate validation queries.
     * @param controlService Object with functions to time, log and stored init configuration
     * @param randomSeed The random seed used for the data generator
     * @throws ClientException
     */
    public CreateValidationParamsMode( ControlService controlService, long randomSeed ) throws ClientException
    {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
        this.randomSeed = randomSeed;
    }

    /**
     * Initializes the validation parameter class. This loads the configuration given through
     * validation.properties and the database to use to generate the validation parameters.
     */
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

    /**
     * Create validation parameters. 
     */
    @Override
    public Object startExecutionAndAwaitCompletion() throws ClientException
    {
        try ( Db db = database )
        {
            Workload w = workload;
            File validationFileToGenerate =
                    new File( controlService.configuration().databaseValidationFilePath() );
                
            int validationSetSize = controlService.configuration().validationParametersSize();

            loggingService.info(
                    format( "Generating database validation file: %s", validationFileToGenerate.getAbsolutePath() ) );

            Iterator<ValidationParam> validationParamsGenerator = new ValidationParamsGenerator(
                    db,
                    w.dbValidationParametersFilter( validationSetSize ),
                    timeMappedOperations,
                    validationSetSize
            );
            List<ValidationParam> validationParams = ImmutableList.copyOf(validationParamsGenerator);
            ValidationParamsToJson validationParamsAsJson = new ValidationParamsToJson(
                validationParams,
                workload,
                controlService.configuration().validationSerializationCheck()
            );
            validationParamsAsJson.serializeValidationParameters(validationFileToGenerate );

            loggingService.info( format( "Successfully generated %s database validation parameters",
                    validationParams.size() ) );
        }
        catch ( Exception e )
        {
            throw new ClientException( "Error encountered duration validation parameter creation", e );
        }
        return null;
    }
}
