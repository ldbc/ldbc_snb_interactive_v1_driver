package com.ldbc.driver.client;

import com.google.common.base.Charsets;
import com.google.common.collect.Iterators;
import com.ldbc.driver.ClientException;
import com.ldbc.driver.Db;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.csv.simple.SimpleCsvFileReader;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.validation.DbValidationResult;
import com.ldbc.driver.validation.DbValidator;
import com.ldbc.driver.validation.ValidationParam;
import com.ldbc.driver.validation.ValidationParamsFromCsvRows;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Iterator;

import static java.lang.String.format;

public class ValidateDatabaseMode implements ClientMode<DbValidationResult>
{
    private final ControlService controlService;
    private final LoggingService loggingService;

    private Workload workload = null;
    private Db database = null;

    public ValidateDatabaseMode( ControlService controlService ) throws ClientException
    {
        this.controlService = controlService;
        this.loggingService = controlService.loggingServiceFactory().loggingServiceFor( getClass().getSimpleName() );
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
        catch ( DbException e )
        {
            throw new ClientException(
                    format( "Error loading DB class: %s", controlService.configuration().dbClassName() ), e );
        }
        loggingService.info( format( "Loaded DB: %s", database.getClass().getName() ) );

        loggingService.info( "Driver Configuration" );
        loggingService.info( controlService.toString() );
    }

    @Override
    public DbValidationResult startExecutionAndAwaitCompletion() throws ClientException
    {
        try ( Workload w = workload; Db db = database )
        {
            File validationParamsFile = new File( controlService.configuration().databaseValidationFilePath() );

            loggingService.info(
                    format( "Validating database against expected results\n * Db: %s\n * Validation Params File: %s",
                            db.getClass().getName(), validationParamsFile.getAbsolutePath() ) );

            int validationParamsCount;
            SimpleCsvFileReader validationParamsReader;
            try
            {
                validationParamsReader = new SimpleCsvFileReader( validationParamsFile,
                        SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
                validationParamsCount = Iterators.size( validationParamsReader );
                validationParamsReader.close();
                validationParamsReader = new SimpleCsvFileReader( validationParamsFile,
                        SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_REGEX_STRING );
            }
            catch ( IOException e )
            {
                throw new ClientException( "Error encountered trying to create CSV file reader", e );
            }

            DbValidationResult databaseValidationResult;
            try
            {
                Iterator<ValidationParam> validationParams =
                        new ValidationParamsFromCsvRows( validationParamsReader, w );
                DbValidator dbValidator = new DbValidator();
                databaseValidationResult = dbValidator.validate(
                        validationParams,
                        db,
                        validationParamsCount,
                        w
                );
            }
            catch ( WorkloadException e )
            {
                throw new ClientException( format( "Error reading validation parameters file\nFile: %s",
                        validationParamsFile.getAbsolutePath() ), e );
            }
            validationParamsReader.close();

            File failedValidationOperationsFile = new File( validationParamsFile.getParentFile(),
                    removeExtension( validationParamsFile.getName() ) + "-failed-actual.json" );
            if ( failedValidationOperationsFile.exists() )
            {
                FileUtils.forceDelete( failedValidationOperationsFile );
            }
            failedValidationOperationsFile.createNewFile();
            try ( Writer writer = new OutputStreamWriter( new FileOutputStream( failedValidationOperationsFile ),
                    Charsets.UTF_8 ) )
            {
                writer.write( databaseValidationResult.actualResultsForFailedOperationsAsJsonString( w ) );
                writer.flush();
                writer.close();
            }
            catch ( Exception e )
            {
                throw new ClientException(
                        format( "Encountered error while writing to file\nFile: %s",
                                failedValidationOperationsFile.getAbsolutePath() ),
                        e
                );
            }

            File expectedResultsForFailedValidationOperationsFile = new File( validationParamsFile.getParentFile(),
                    removeExtension( validationParamsFile.getName() ) + "-failed-expected.json" );
            if ( expectedResultsForFailedValidationOperationsFile.exists() )
            {
                FileUtils.forceDelete( expectedResultsForFailedValidationOperationsFile );
            }
            expectedResultsForFailedValidationOperationsFile.createNewFile();
            try ( Writer writer = new OutputStreamWriter(
                    new FileOutputStream( expectedResultsForFailedValidationOperationsFile ), Charsets.UTF_8 ) )
            {
                writer.write( databaseValidationResult.expectedResultsForFailedOperationsAsJsonString( w ) );
                writer.flush();
                writer.close();
            }
            catch ( Exception e )
            {
                throw new ClientException(
                        format( "Encountered error while writing to file\nFile: %s",
                                failedValidationOperationsFile.getAbsolutePath() ),
                        e
                );
            }

            loggingService.info( databaseValidationResult.resultMessage() );
            loggingService.info( format(
                    "For details see the following files:\n * %s\n * %s",
                    failedValidationOperationsFile.getAbsolutePath(),
                    expectedResultsForFailedValidationOperationsFile.getAbsolutePath()
            ) );
            return databaseValidationResult;
        }
        catch ( IOException e )
        {
            throw new ClientException( "Error occurred during database validation", e );
        }
    }

    String removeExtension( String filename )
    {
        return (filename.indexOf( "." ) == -1) ? filename : filename.substring( 0, filename.lastIndexOf( "." ) );
    }

}
