package com.ldbc.driver;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.ldbc.driver.db.basic.BasicDb;
import com.ldbc.driver.generator.GeneratorBuilder;
import com.ldbc.driver.measurements.Measurements;
import com.ldbc.driver.measurements.MeasurementsException;
import com.ldbc.driver.measurements.exporter.MeasurementsExporter;
import com.ldbc.driver.measurements.exporter.TextMeasurementsExporter;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Pair;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.workloads.Workload;
import com.ldbc.driver.workloads.WorkloadException;

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

    private static final String WORKLOAD = "workload";
    private static final String EXPORT_FILE_PATH = "exportfile";
    private static final String OPERATION_COUNT = "operationcount";
    private static final String OPERATION_COUNT_DEFAULT = "0";
    private static final String EXPORTER = "exporter";
    private static final String EXPORTER_DEFAULT = TextMeasurementsExporter.class.getName();
    private static final String STATUS = "status";
    private static final String STATUS_DEFAULT = "false";
    private static final String DB = "db";
    private static final String DB_DEFAULT = BasicDb.class.getName();
    private static final String TARGET_THROUGHPUT = "target_throughput";
    private static final String TARGET_THROUGHPUT_DEFAULT = "0";
    private static final String BENCHMARK_PHASE = "benchmark_phase";
    private static final String BENCHMARK_PHASE_DEFAULT = BenchmarkPhase.TRANSACTION_PHASE.toString();
    private static final String THREAD_COUNT = "thread_count";
    private static final String THREAD_COUNT_DEFAULT = Integer.toString( defaultThreadCount() );

    private static final String[] REQUIRED_PROPERTIES = new String[] { WORKLOAD };

    private static final long RANDOM_SEED = 42;

    public static void main( String[] args ) throws ClientException
    {
        Client client = new Client();
        try
        {
            logger.info( "LDBC Driver 0.1" );
            StringBuilder welcomeMessage = new StringBuilder();
            welcomeMessage.append( "Command line:" );
            for ( int i = 0; i < args.length; i++ )
            {
                welcomeMessage.append( " " + args[i] );
            }
            logger.info( welcomeMessage );

            Map<String, String> properties = parseArguments( args );

            client.start( properties );
        }
        catch ( ClientException e )
        {
            String errMsg = "Error while trying to parse properties";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
        catch ( Exception e )
        {
            logger.error( "Client terminated unexpectedly", e );
        }
        finally
        {
            client.exit();
        }
    }

    private static int defaultThreadCount()
    {
        // Client & OperationResultLoggingThread
        int threadsUsedByDriver = 2;
        int totalProcessors = Runtime.getRuntime().availableProcessors();
        int availableThreads = totalProcessors - threadsUsedByDriver;
        return Math.max( 1, availableThreads );
    }

    private static String usageMessage()
    {
        String usageMessage = "Usage: java com.ldbc.driver.Client [options]\n"

        + "Options:\n"

        + "  -threads n: execute using n threads (default: 1) - can also be specified as the \n"

        + "              \"threadcount\" property using -p\n"

        + "  -target n: attempt to do n operations per second (default: unlimited) - can also\n"

        + "             be specified as the \"target\" property using -p\n"

        + "  -load:  run the loading phase of the workload\n"

        + "  -t:  run the transactions phase of the workload (default)\n"

        + "  -db dbname: specify the name of the DB to use (default: com.ldbc.driver.db.BasicDB) - \n"

        + "              can also be specified as the \"db\" property using -p\n"

        + "  -P propertyfile: load properties from the given file. Multiple files can\n"

        + "                   be specified, and will be processed in the order specified\n"

        + "  -p name=value:  specify a property to be passed to the DB and workloads;\n"

        + "                  multiple properties can be specified, and override any\n"

        + "                  values in the propertyfile\n"

        + "  -s:  show status during run (default: no status)\n"

        + "  -l label:  use label for status (e.g. to label one experiment out of a whole batch)\n"

        + "\nRequired properties:\n"

        + "  " + WORKLOAD + ": the name of the workload class to use (e.g. com.ldbc.driver.workloads.CoreWorkload)\n"

        + "\nTo run the transaction phase from multiple servers, start a separate client on each."

        + "To run the load phase from multiple servers, start a separate client on each; additionally,\n"

        + "use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted";

        return usageMessage;
    }

    private static Map<String, String> parseArguments( String[] args ) throws ClientException
    {
        Map<String, String> commandlineProperties = new HashMap<String, String>();
        Properties fileProperties = new Properties();

        int argIndex = 0;

        if ( args.length == 0 )
        {
            String errMsg = usageMessage();
            logger.info( errMsg );
            throw new ClientException();
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
                    String errMsg = String.format( "Error loading properties file [%s]", argPropertiesFile );
                    logger.error( errMsg, e );
                    throw new ClientException( errMsg, e.getCause() );
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
                String errMsg = "Unknown option " + args[argIndex];
                logger.info( errMsg );
                logger.info( usageMessage() );
                throw new ClientException( errMsg );
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

    private void start( Map<String, String> properties ) throws ClientException
    {
        Pair<Boolean, String> isRequiredProperties = checkRequiredProperties( properties, REQUIRED_PROPERTIES );
        if ( false == isRequiredProperties._1() )
        {
            String errMsg = isRequiredProperties._2();
            logger.info( errMsg );
            throw new ClientException( errMsg );
        }

        GeneratorBuilder generatorBuilder = new GeneratorBuilder( new RandomDataGeneratorFactory( RANDOM_SEED ) );

        boolean showStatus = Boolean.parseBoolean( MapUtils.mapGetDefault( properties, STATUS, STATUS_DEFAULT ) );
        logger.info( String.format( "Show status: %s", showStatus ) );

        int threadCount = Integer.parseInt( MapUtils.mapGetDefault( properties, THREAD_COUNT, THREAD_COUNT_DEFAULT ) );
        logger.info( String.format( "Thread count: %s", threadCount ) );

        BenchmarkPhase benchmarkPhase = BenchmarkPhase.valueOf( MapUtils.mapGetDefault( properties, BENCHMARK_PHASE,
                BENCHMARK_PHASE_DEFAULT ) );
        logger.info( String.format( "Benchmark phase: %s", benchmarkPhase ) );

        // compute the target throughput
        int targetThroughput = Integer.parseInt( MapUtils.mapGetDefault( properties, TARGET_THROUGHPUT,
                TARGET_THROUGHPUT_DEFAULT ) );
        double targetThroughputPerMs = -1;
        if ( targetThroughput > 0 )
        {
            targetThroughputPerMs = targetThroughput / 1000.0;
        }
        logger.info( String.format( "Target throughput: %s", targetThroughput ) );

        // TODO change the way this is done! no static
        Measurements.setProperties( properties );
        Measurements measurements = Measurements.getMeasurements();
        logger.info( String.format( "Measurements: %s", measurements.getClass().getName() ) );

        Workload workload = null;
        String workloadName = properties.get( WORKLOAD );
        try
        {
            workload = ClassLoaderHelper.loadWorkload( workloadName );
            workload.init( properties );
        }
        catch ( Exception e )
        {
            String errMsg = String.format( "Error loading Workload class: %s", workloadName );
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );

        }
        logger.info( String.format( "Loaded Workload: %s", workload.getClass().getName() ) );

        Db db = null;
        String dbName = MapUtils.mapGetDefault( properties, DB, DB_DEFAULT );
        try
        {
            db = ClassLoaderHelper.loadDb( dbName );
            db.init( properties );
        }
        catch ( DbException e )
        {
            String errMsg = String.format( "Error loading DB class: %s", dbName );
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
        logger.info( String.format( "Loaded DB: %s", db.getClass().getName() ) );

        int operationCount = getOperationCount( properties, benchmarkPhase );
        WorkloadRunner workloadRunner = new WorkloadRunner( db, benchmarkPhase, workload, operationCount,
                targetThroughputPerMs, generatorBuilder, showStatus, threadCount, measurements );

        logger.info( String.format( "Starting Benchmark (%s operations)", operationCount ) );
        long st = System.currentTimeMillis();
        try
        {
            workloadRunner.run();
        }
        catch ( ClientException e )
        {
            String errMsg = "Error running Workload";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
        long en = System.currentTimeMillis();

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

        logger.info( "Exporting Measurements..." );
        try
        {
            exportMeasurements( properties, operationCount, en - st );
        }
        catch ( MeasurementsException e )
        {
            String errMsg = "Could not export Measurements";
            logger.error( errMsg, e );
            throw new ClientException( errMsg, e.getCause() );
        }
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

    private void exit()
    {
        // TODO YCSB used System.exit(0) to kill its many driver threads. those
        // threads no longer exist, but others will at the DB connection
        // layer. What's the cleanest/safest/right way to terminate the
        // application and clean up all threads?
        System.exit( 0 );
    }

    /**
     * Exports measurements using MeasurementsExporter loaded from config
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
