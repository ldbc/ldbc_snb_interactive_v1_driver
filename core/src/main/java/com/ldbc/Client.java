package com.ldbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.ldbc.db.basic.BasicDb;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.measurements.Measurements;
import com.ldbc.measurements.MeasurementsException;
import com.ldbc.measurements.exporter.MeasurementsExporter;
import com.ldbc.measurements.exporter.TextMeasurementsExporter;
import com.ldbc.util.ClassLoaderHelper;
import com.ldbc.util.Pair;
import com.ldbc.util.RandomDataGeneratorFactory;
import com.ldbc.util.MapUtils;
import com.ldbc.workloads.Workload;
import com.ldbc.workloads.WorkloadException;

public class Client
{
    private static Logger logger = Logger.getLogger( Client.class );

    /*
     * For partitioning load among machines when client is bottleneck.
    * 
     * INSERT_START
     * Specifies which record ID each client starts from - enables load phase to proceed from 
     * multiple clients on different machines.
     * 
     * INSERT_COUNT
     * Specifies number of inserts each client should do, if less than RECORD_COUNT.
     * Works in conjunction with INSERT_START, which specifies the record to start at (offset).
     *  
     * E.g. to load 1,000,000 records from 2 machines: 
     * client 1 --> insertStart=0
     *          --> insertCount=500,000
     * client 2 --> insertStart=50,000
     *          --> insertCount=500,000
    */
    public static final String INSERT_COUNT = "insertcount";
    public static final String INSERT_COUNT_DEFAULT = "0";
    public static final String INSERT_START = "insertstart";
    public static final String INSERT_START_DEFAULT = "0";
    public static final String RECORD_COUNT = "recordcount";
    public static final String RECORD_COUNT_DEFAULT = "0";

    private final String WORKLOAD = "workload";
    private final String EXPORT_FILE_PATH = "exportfile";
    private final String OPERATION_COUNT = "operationcount";
    private final String OPERATION_COUNT_DEFAULT = "0";
    private final String EXPORTER = "exporter";
    private final String EXPORTER_DEFAULT = TextMeasurementsExporter.class.getName();
    private final String STATUS = "status";
    private final String STATUS_DEFAULT = "false";
    private final String DB = "db";
    private final String DB_DEFAULT = BasicDb.class.getName();
    private final String TARGET_THROUGHPUT = "target_throughput";
    private final String TARGET_THROUGHPUT_DEFAULT = "0";
    private final String BENCHMARK_PHASE = "benchmark_phase";
    private final String BENCHMARK_PHASE_DEFAULT = BenchmarkPhase.TRANSACTION_PHASE.toString();

    private final String[] requiredProperties = new String[] { WORKLOAD };

    public static void main( String[] args ) throws ClientException
    {
        Client client = new Client();
        client.start( args );
        client.exit();
    }

    private void start( String[] args )
    {
        long seed = System.currentTimeMillis();
        RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory( seed );
        GeneratorBuilder generatorBuilder = new GeneratorBuilder( randomFactory );

        Map<String, String> commandlineProperties = null;
        try
        {
            commandlineProperties = parseArguments( args );
        }
        catch ( ClientException e )
        {
            logger.info( "Error while try to parse properties", e.getCause() );
            exit();
        }

        Pair<Boolean, String> isRequiredProperties = checkRequiredProperties( commandlineProperties, requiredProperties );
        if ( false == isRequiredProperties._1() )
        {
            String errMsg = isRequiredProperties._2();
            logger.info( errMsg );
            exit();
        }

        boolean showStatus = Boolean.parseBoolean( MapUtils.mapGetDefault( commandlineProperties, STATUS,
                STATUS_DEFAULT ) );

        BenchmarkPhase benchmarkPhase = BenchmarkPhase.valueOf( MapUtils.mapGetDefault( commandlineProperties,
                BENCHMARK_PHASE, BENCHMARK_PHASE_DEFAULT ) );

        // compute the target throughput
        int targetThroughput = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, TARGET_THROUGHPUT,
                TARGET_THROUGHPUT_DEFAULT ) );
        double targetThroughputPerMs = -1;
        if ( targetThroughput > 0 )
        {
            targetThroughputPerMs = targetThroughput / 1000.0;
        }

        logger.info( "YCSB Client 0.1" );
        logger.info( "Command line:" );
        for ( int i = 0; i < args.length; i++ )
        {
            logger.info( " " + args[i] );
        }
        logger.info( "\nLoading workload..." );

        Measurements.setProperties( commandlineProperties );

        Workload workload = null;
        String workloadName = commandlineProperties.get( WORKLOAD );
        try
        {
            workload = ClassLoaderHelper.loadWorkload( workloadName );
            workload.init( commandlineProperties );
        }
        catch ( Exception e )
        {
            logger.info( String.format( "Error loading Workload class: %s", workloadName ), e.getCause() );
            exit();
        }

        Db db = null;
        String dbName = MapUtils.mapGetDefault( commandlineProperties, DB, DB_DEFAULT );
        try
        {
            db = ClassLoaderHelper.loadDb( dbName );
            db.init( commandlineProperties );
        }
        catch ( DbException e )
        {
            logger.info( String.format( "Error loading DB class: %s", dbName ), e.getCause() );
            exit();
        }

        logger.info( "Starting Benchmark" );

        int operationCount = getOperationCount( commandlineProperties, benchmarkPhase );

        WorkloadRunner workloadRunner = new WorkloadRunner( db, benchmarkPhase, workload, operationCount,
                targetThroughputPerMs, generatorBuilder, showStatus );

        long st = System.currentTimeMillis();

        try
        {
            workloadRunner.run();
        }
        catch ( ClientException e )
        {
            logger.info( "Error running benchmark", e.getCause() );
            exit();
        }

        long en = System.currentTimeMillis();

        try
        {
            workload.cleanup();
        }
        catch ( WorkloadException e )
        {
            logger.info( "Error during Workload cleanup", e.getCause() );
            exit();
        }

        try
        {
            db.cleanup();
        }
        catch ( DbException e )
        {
            logger.info( "Error during DB cleanup", e.getCause() );
            exit();
        }

        try
        {
            exportMeasurements( commandlineProperties, operationCount, en - st );
        }
        catch ( MeasurementsException e )
        {
            logger.info( "Could not export measurements", e.getCause() );
            exit();
        }
    }

    private int getOperationCount( Map<String, String> commandlineProperties, BenchmarkPhase benchmarkPhase )
            throws NumberFormatException
    {
        int operationCount = 0;
        switch ( benchmarkPhase )
        {
        case TRANSACTION_PHASE:
            operationCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, OPERATION_COUNT,
                    OPERATION_COUNT_DEFAULT ) );
            break;

        case LOAD_PHASE:
            if ( commandlineProperties.containsKey( INSERT_COUNT ) )
            {
                operationCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, INSERT_COUNT,
                        INSERT_COUNT_DEFAULT ) );
            }
            else
            {
                operationCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, RECORD_COUNT,
                        RECORD_COUNT_DEFAULT ) );
            }
            break;
        }
        return operationCount;
    }

    private Map<String, String> parseArguments( String[] args ) throws ClientException
    {
        Map<String, String> commandlineProperties = new HashMap<String, String>();
        Properties fileProperties = new Properties();

        int argIndex = 0;

        if ( args.length == 0 )
        {
            logger.info( usageMessage() );
            exit();
        }

        while ( args[argIndex].startsWith( "-" ) )
        {
            if ( args[argIndex].equals( "-target" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    logger.info( usageMessage() );
                }
                int argTarget = Integer.parseInt( args[argIndex] );
                commandlineProperties.put( TARGET_THROUGHPUT, Integer.toString( argTarget ) );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-load" ) )
            {
                commandlineProperties.put( BENCHMARK_PHASE, BenchmarkPhase.LOAD_PHASE.toString() );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-t" ) )
            {
                commandlineProperties.put( BENCHMARK_PHASE, BenchmarkPhase.TRANSACTION_PHASE.toString() );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-s" ) )
            {
                commandlineProperties.put( STATUS, "true" );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-db" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    logger.info( usageMessage() );
                }
                String argDb = args[argIndex];
                commandlineProperties.put( DB, argDb );
                argIndex++;
            }
            else if ( args[argIndex].equals( "-P" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    logger.info( usageMessage() );
                }
                String argPropertiesFile = args[argIndex];
                argIndex++;

                try
                {
                    fileProperties.load( new FileInputStream( argPropertiesFile ) );
                }
                catch ( IOException e )
                {
                    throw new ClientException(
                            String.format( "Error loading properties file [%s]", argPropertiesFile ), e.getCause() );
                }
            }
            else if ( args[argIndex].equals( "-p" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    logger.info( usageMessage() );
                }
                int equalsCharPosition = args[argIndex].indexOf( '=' );
                if ( equalsCharPosition < 0 )
                {
                    logger.info( usageMessage() );
                }

                String argPropertyName = args[argIndex].substring( 0, equalsCharPosition );
                String argPropertyValue = args[argIndex].substring( equalsCharPosition + 1 );
                commandlineProperties.put( argPropertyName, argPropertyValue );
                argIndex++;
            }
            else
            {
                logger.info( "Unknown option " + args[argIndex] );
                logger.info( usageMessage() );
                exit();
            }

            if ( argIndex >= args.length )
            {
                break;
            }
        }

        if ( argIndex != args.length )
        {
            logger.info( usageMessage() );
        }

        return MapUtils.mergePropertiesToMap( fileProperties, commandlineProperties, false );
    }

    private void exit()
    {
        // TODO YCSB used System.exit(0) to kill its many driver threads. those
        // threads no longer exist, but others will at the DB connection
        // layer. What's the cleanest/safest/right way to terminate the
        // application and clean up all threads?
        System.exit( 0 );
    }

    private String usageMessage()
    {
        String usageMessage = "Usage: java com.yahoo.ycsb.Client [options]\n"

        + "Options:\n"

        + "  -threads n: execute using n threads (default: 1) - can also be specified as the \n"

        + "              \"threadcount\" property using -p\n"

        + "  -target n: attempt to do n operations per second (default: unlimited) - can also\n"

        + "             be specified as the \"target\" property using -p\n"

        + "  -load:  run the loading phase of the workload\n"

        + "  -t:  run the transactions phase of the workload (default)\n"

        + "  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n"

        + "              can also be specified as the \"db\" property using -p\n"

        + "  -P propertyfile: load properties from the given file. Multiple files can\n"

        + "                   be specified, and will be processed in the order specified\n"

        + "  -p name=value:  specify a property to be passed to the DB and workloads;\n"

        + "                  multiple properties can be specified, and override any\n"

        + "                  values in the propertyfile\n"

        + "  -s:  show status during run (default: no status)\n"

        + "  -l label:  use label for status (e.g. to label one experiment out of a whole batch)\n"

        + "\nRequired properties:\n"

        + "  " + WORKLOAD + ": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)\n"

        + "\nTo run the transaction phase from multiple servers, start a separate client on each."

        + "To run the load phase from multiple servers, start a separate client on each; additionally,\n"

        + "use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted";

        return usageMessage;
    }

    private Pair<Boolean, String> checkRequiredProperties( Map<String, String> properties, String[] requiredProperties )
    {
        for ( String property : requiredProperties )
        {
            if ( false == properties.containsKey( property ) )
            {
                return Pair.create( false, "Missing property: " + property );
            }
        }
        return Pair.create( true, "" );
    }

    /**
     * Exports measurements using MeasurementsExporter loaded from config
     * 
     * @throws MeasurementsException
     */
    private void exportMeasurements( Map<String, String> properties, int opcount, long runtime )
            throws MeasurementsException
    {
        MeasurementsExporter exporter = null;
        try
        {
            String exportFilePath = properties.get( EXPORT_FILE_PATH );
            OutputStream out = null;
            try
            {
                out = ( exportFilePath == null ) ? System.out : new FileOutputStream( exportFilePath );
            }
            catch ( FileNotFoundException e )
            {
                throw new MeasurementsException( String.format( "Could not find file [%s]", exportFilePath ),
                        e.getCause() );
            }

            String exporterClassName = MapUtils.mapGetDefault( properties, EXPORTER, EXPORTER_DEFAULT );
            try
            {
                exporter = ClassLoaderHelper.loadMeasurementsExporter( exporterClassName, out );
            }
            catch ( Exception e )
            {
                throw new MeasurementsException( String.format( "Could not find exporter [%s]", exporterClassName ),
                        e.getCause() );
            }

            exporter.write( "OVERALL", "RunTime(ms)", runtime );
            double throughput = 1000.0 * ( (double) opcount ) / ( (double) runtime );
            exporter.write( "OVERALL", "Throughput(ops/sec)", throughput );

            Measurements.getMeasurements().exportMeasurements( exporter );
        }
        finally
        {
            if ( exporter != null )
            {
                try
                {
                    exporter.close();
                }
                catch ( IOException e )
                {
                    throw new MeasurementsException( "Error closing exporter", e.getCause() );
                }
            }
        }
    }
}
