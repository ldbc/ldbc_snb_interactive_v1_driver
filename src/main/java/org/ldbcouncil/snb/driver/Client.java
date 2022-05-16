package org.ldbcouncil.snb.driver;
/**
 * Client.java
 * 
 * Entrypoint for the SNB Driver. This class creates default control classes,
 * checks which driver mode is specified and starts the application.
 * There are 4 supported modes (in order of priority):
 * 1. Create validation parameters
 * 2. Validate database
 * 3. Create workload statistics
 * 4. Execute Benchmark
 * 
 * To print the usage help for the driver, use help = true in properties file.
 */

import org.ldbcouncil.snb.driver.client.CalculateWorkloadStatisticsMode;
import org.ldbcouncil.snb.driver.client.ClientMode;
import org.ldbcouncil.snb.driver.client.CreateValidationParamsMode;
import org.ldbcouncil.snb.driver.client.ExecuteWorkloadMode;
import org.ldbcouncil.snb.driver.client.PrintHelpMode;
import org.ldbcouncil.snb.driver.client.ValidateDatabaseMode;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.ControlService;
import org.ldbcouncil.snb.driver.control.DriverConfiguration;
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.control.LocalControlService;
import org.ldbcouncil.snb.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.control.LoggingServiceFactory;
import org.ldbcouncil.snb.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.snb.driver.temporal.SystemTimeSource;
import org.ldbcouncil.snb.driver.temporal.TimeSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

// TODO Validate Workload to work with short reads

public class Client
{
    private static final long RANDOM_SEED = 42;

    public static void main( String[] args ) throws ClientException
    {
        ControlService controlService = null;
        boolean detailedStatus = false;
        LoggingServiceFactory loggingServiceFactory = new Log4jLoggingServiceFactory( detailedStatus );
        LoggingService loggingService = loggingServiceFactory.loggingServiceFor( Client.class.getSimpleName() );
        try
        {
            TimeSource systemTimeSource = new SystemTimeSource();
            ConsoleAndFileDriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromArgs( args );
            // TODO this method will not work with multiple processes - should come from controlService
            long workloadStartTimeAsMilli = systemTimeSource.nowAsMilli() + TimeUnit.SECONDS.toMillis( 5 );
            controlService = new LocalControlService(
                    workloadStartTimeAsMilli,
                    configuration,
                    loggingServiceFactory,
                    systemTimeSource );
            Client client = new Client();
            ClientMode clientMode = client.getClientModeFor( controlService );
            clientMode.init();
            clientMode.startExecutionAndAwaitCompletion();
        }
        catch ( DriverConfigurationException e )
        {
            String errMsg = format( "Error parsing parameters: %s", e.getMessage() );
            loggingService.info( errMsg );
            System.exit( 1 );
        }
        catch ( Exception e )
        {
            loggingService.info( "Client terminated unexpectedly\n" + ConcurrentErrorReporter.stackTraceToString( e ) );
            System.exit( 1 );
        }
        finally
        {
            if ( null != controlService )
            {
                controlService.shutdown();
            }
        }
    }

    // TODO should not be doing things like ConsoleAndFileDriverConfiguration.DB_ARG
    // TODO ConsoleAndFileDriverConfiguration could maybe have a DriverParam(enum)-to-String(arg) method?
    public ClientMode getClientModeFor( ControlService controlService ) throws ClientException
    {
        if ( controlService.configuration().shouldPrintHelpString() )
        {
            // Print Help
            return new PrintHelpMode( controlService );
        }
        else if (controlService.configuration().validationCreationParams() )
        {
            // Create Validation Parameters
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();

            if ( null == configuration.dbClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.DB_ARG );
            }
            if ( null == configuration.workloadClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG );
            }
            if ( 0 == configuration.operationCount() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG );
            }
            if ( false == missingParams.isEmpty() )
            {
                throw new ClientException( format( "Missing required parameters: %s", missingParams.toString() ) );
            }
            return new CreateValidationParamsMode( controlService, RANDOM_SEED );
        }
        else if ( null != controlService.configuration().databaseValidationFilePath() )
        {
            // Validate Database
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if ( null == configuration.dbClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.DB_ARG );
            }
            if ( null == configuration.workloadClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG );
            }
            if ( false == missingParams.isEmpty() )
            {
                throw new ClientException( format( "Missing required parameters: %s", missingParams.toString() ) );
            }
            return new ValidateDatabaseMode( controlService );
        }
        else if ( controlService.configuration().calculateWorkloadStatistics() )
        {
            // Calculate Statistics
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if ( null == configuration.workloadClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG );
            }
            if ( 0 == configuration.operationCount() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG );
            }
            if ( false == missingParams.isEmpty() )
            {
                throw new ClientException( format( "Missing required parameters: %s", missingParams.toString() ) );
            }
            return new CalculateWorkloadStatisticsMode( controlService, RANDOM_SEED );
        }
        else
        {
            // Execute Workload
            DriverConfiguration configuration = controlService.configuration();
            List<String> missingParams = new ArrayList<>();
            if ( null == configuration.dbClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.DB_ARG );
            }
            if ( null == configuration.workloadClassName() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG );
            }
            if ( 0 == configuration.operationCount() )
            {
                missingParams.add( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG );
            }
            if ( false == missingParams.isEmpty() )
            {
                throw new ClientException( format( "Missing required parameters: %s", missingParams.toString() ) );
            }
            return new ExecuteWorkloadMode( controlService, new SystemTimeSource(), RANDOM_SEED );
        }
    }
}