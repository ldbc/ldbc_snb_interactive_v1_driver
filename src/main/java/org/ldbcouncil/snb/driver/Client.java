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
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.control.LocalControlService;
import org.ldbcouncil.snb.driver.control.Log4jLoggingServiceFactory;
import org.ldbcouncil.snb.driver.control.LoggingService;
import org.ldbcouncil.snb.driver.control.LoggingServiceFactory;
import org.ldbcouncil.snb.driver.control.OperationMode;
import org.ldbcouncil.snb.driver.runtime.ConcurrentErrorReporter;
import org.ldbcouncil.snb.driver.temporal.SystemTimeSource;
import org.ldbcouncil.snb.driver.temporal.TimeSource;

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

    /**
     * Create instance of operation mode. 
     * @param controlService ControlService with loaded DriverConfiguration
     * @return ClientMode object with specified operation mode
     * @throws ClientException When one or more required parameters are missing
     */
    public ClientMode getClientModeFor( ControlService controlService ) throws ClientException
    {
        if ( controlService.configuration().shouldPrintHelpString() )
        {
            return new PrintHelpMode( controlService );
        }

        List<String> missingParams = 
            ConsoleAndFileDriverConfiguration.checkMissingParams(controlService.configuration());
        if ( !missingParams.isEmpty() )
        {
            throw new ClientException( format( "Missing required parameters: %s", missingParams.toString() ) );
        }

        OperationMode mode = OperationMode.valueOf(controlService.configuration().mode());

        switch (mode) {
            case create_validation:
                return new CreateValidationParamsMode( controlService, RANDOM_SEED );
            case create_statistics:
                return new CalculateWorkloadStatisticsMode( controlService, RANDOM_SEED );
            case validate_database:
                return new ValidateDatabaseMode( controlService );
            case execute_benchmark:
            default: // Execute benchmark is default behaviour
                return new ExecuteWorkloadMode( controlService, new SystemTimeSource(), RANDOM_SEED );
        }
    }
}
