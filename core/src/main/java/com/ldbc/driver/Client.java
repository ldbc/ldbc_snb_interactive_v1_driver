package com.ldbc.driver;

import org.apache.log4j.Logger;

import com.ldbc.driver.generator.Generator;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.measurements.MetricsExporterException;
import com.ldbc.driver.measurements.WorkloadMetricsManager;
import com.ldbc.driver.measurements.exporters.OutputStreamMetricsExporter;
import com.ldbc.driver.measurements.formatters.DiscreteMetricSimpleFormatter;
import com.ldbc.driver.measurements.formatters.HdrHistogramMetricSimpleFormatter;
import com.ldbc.driver.runner.WorkloadRunner;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.temporal.Time;
import com.ldbc.driver.util.temporal.TimeUnit;

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
            String errMsg = "Error parse parameters";
            logger.error( errMsg, e );
        }
        catch ( Exception e )
        {
            logger.error( "Client terminated unexpectedly", e );
        }
        finally
        {
            // TODO is this the cleanest/safest/right way to cleanup?
            System.exit( 0 );
        }
    }

    public void start( WorkloadParams params ) throws ClientException
    {
        logger.info( "LDBC Workload Driver" );
        logger.info( params.toString() );

        GeneratorBuilder generatorBuilder = new GeneratorBuilder( new RandomDataGeneratorFactory( RANDOM_SEED ) );

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

        TimeUnit durationUnit = TimeUnit.NANO;
        WorkloadMetricsManager metricsManager = new WorkloadMetricsManager( durationUnit );

        WorkloadRunner workloadRunner = null;
        try
        {
            Generator<Operation<?>> operationGenerator = getOperationGenerator( workload, params.getBenchmarkPhase(),
                    generatorBuilder );
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
            metricsManager.export( new OutputStreamMetricsExporter( System.out ),
                    new HdrHistogramMetricSimpleFormatter(), new DiscreteMetricSimpleFormatter() );

        }
        catch ( MetricsExporterException e )
        {
            String errMsg = "Could not export Measurements";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
    }

    private Generator<Operation<?>> getOperationGenerator( Workload workload, BenchmarkPhase benchmarkPhase,
            GeneratorBuilder generatorBuilder ) throws WorkloadException
    {
        switch ( benchmarkPhase )
        {
        case LOAD_PHASE:
            return workload.getLoadOperations( generatorBuilder );
        case TRANSACTION_PHASE:
            return workload.getTransactionalOperations( generatorBuilder );
        }
        throw new WorkloadException( "Error encounterd trying to get operation generator" );
    }
}
