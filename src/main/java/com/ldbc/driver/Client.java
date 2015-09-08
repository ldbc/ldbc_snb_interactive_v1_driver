package com.ldbc.driver;

import com.ldbc.driver.client.CalculateWorkloadStatisticsMode;
import com.ldbc.driver.client.ClientMode;
import com.ldbc.driver.client.CreateValidationParamsMode;
import com.ldbc.driver.client.ExecuteWorkloadMode;
import com.ldbc.driver.client.PrintHelpMode;
import com.ldbc.driver.client.ValidateDatabaseMode;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.ControlService;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.control.LocalControlService;
import com.ldbc.driver.control.Log4jLoggingServiceFactory;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.control.LoggingServiceFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.TimeSource;

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
        else if ( null != controlService.configuration().validationParamsCreationOptions() )
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