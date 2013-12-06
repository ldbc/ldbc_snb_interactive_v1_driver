package com.ldbc.driver;

import java.util.Iterator;

import org.apache.log4j.Logger;

import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.measurements.WorkloadMetricsManager;
import com.ldbc.driver.measurements.formatters.SimpleOperationMetricsFormatter;
import com.ldbc.driver.runner.WorkloadRunner;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.temporal.Time;

public class Client
{
    private static Logger logger = Logger.getLogger( Client.class );

    private static final long RANDOM_SEED = 42;

    public static void main( String[] args ) throws ClientException
    {
        Client client = new Client();
        try
        {
            WorkloadParams params = WorkloadParams.fromArgs( args );
            client.start( params );
        }
        catch ( ParamsException e )
        {
            String errMsg = String.format( "Error parsing parameters: %s", e.getMessage() );
            logger.error( errMsg );
        }
        catch ( Exception e )
        {
            logger.error( "Client terminated unexpectedly", e );
        }
    }

    public void start( WorkloadParams params ) throws ClientException
    {
        logger.info( "LDBC Workload Driver" );
        logger.info( params.toString() );

        GeneratorFactory generators = new GeneratorFactory( new RandomDataGeneratorFactory( RANDOM_SEED ) );

        Workload workload = null;
        try
        {
            workload = ClassLoaderHelper.loadWorkload( params.getWorkloadClassName() );
            workload.init( params );
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error loading Workload class: %s", params.getWorkloadClassName() );
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );

        }
        logger.info( String.format( "Loaded Workload: %s", workload.getClass().getName() ) );

        Db db = null;
        try
        {
            db = ClassLoaderHelper.loadDb( params.getDbClassName() );
            db.init( params.asMap() );
        }
        catch ( DbException e )
        {
            String errMsg = String.format( "Error loading DB class: %s", params.getDbClassName() );
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
        logger.info( String.format( "Loaded DB: %s", db.getClass().getName() ) );

        WorkloadMetricsManager metricsManager = new WorkloadMetricsManager( params.getTimeUnit() );

        WorkloadRunner workloadRunner = null;
        try
        {
            Iterator<Operation<?>> operationGenerator = getOperationGenerator( workload, params.getBenchmarkPhase(),
                    generators );
            workloadRunner = new WorkloadRunner( db, operationGenerator, params.isShowStatus(),
                    params.getThreadCount(), metricsManager );
        }
        catch ( WorkloadException e )
        {
            String errMsg = "Error instantiating WorkloadRunner";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }

        logger.info( String.format( "Starting Benchmark (%s operations)", params.getOperationCount() ) );
        Time startTime = Time.now();
        try
        {
            workloadRunner.run();
        }
        catch ( WorkloadException e )
        {
            String errMsg = "Error running Workload";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
        Time endTime = Time.now();

        logger.info( "Cleaning up Workload..." );
        try
        {
            workload.cleanup();
        }
        catch ( WorkloadException e )
        {
            String errMsg = "Error during Workload cleanup";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }

        logger.info( "Cleaning up DB..." );
        try
        {
            db.cleanup();
        }
        catch ( DbException e )
        {
            String errMsg = "Error during DB cleanup";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }

        logger.info( String.format( "Runtime: %s (s)", endTime.greaterBy( startTime ).asSeconds() ) );
        logger.info( "Exporting Measurements..." );
        try
        {
            metricsManager.export( new SimpleOperationMetricsFormatter(), System.out );

        }
        catch ( WorkloadException e )
        {
            String errMsg = "Could not export Measurements";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
    }

    private Iterator<Operation<?>> getOperationGenerator( Workload workload, BenchmarkPhase benchmarkPhase,
            GeneratorFactory generators ) throws WorkloadException
    {
        switch ( benchmarkPhase )
        {
        case LOAD_PHASE:
            return workload.getLoadOperations( generators );
        case TRANSACTION_PHASE:
            return workload.getTransactionalOperations( generators );
        }
        throw new WorkloadException( "Error encounterd trying to get operation generator" );
    }
}
