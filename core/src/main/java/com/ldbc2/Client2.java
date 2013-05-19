package com.ldbc2;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import com.ldbc.db2.DbFactory2;
import com.ldbc.db2.Db2;
import com.ldbc.db2.UnknownDBException2;
import com.ldbc.generator.GeneratorBuilder;
import com.ldbc.measurements.Measurements;
import com.ldbc.measurements.exporter.MeasurementsExporter;
import com.ldbc.measurements.exporter.TextMeasurementsExporter;
import com.ldbc.util.RandomDataGeneratorFactory;
import com.ldbc.util.MapUtils;
import com.ldbc.workloads2.Workload2;

/**
 * Main class for executing LDBC Benchmark Driver
 */
public class Client2
{
    public static final String OPERATION_COUNT = "operationcount";
    public static final String RECORD_COUNT = "recordcount";
    public static final String WORKLOAD = "workload";
    public static final String EXPORTER = "exporter";
    public static final String EXPORT_FILE_PATH = "exportfile";

    public static final String[] requiredProperties = new String[] { WORKLOAD };

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
    public static final String INSERT_START = "insertstart";
    public static final String INSERT_START_DEFAULT = "0";

    public static String usageMessage()
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

    public static boolean checkRequiredProperties( Map<String, String> properties, String[] requiredProperties )
    {
        for ( String property : requiredProperties )
        {
            if ( false == properties.containsKey( property ) )
            {
                // TODO use logger
                System.out.println( "Missing property: " + WORKLOAD );
                return false;
            }
        }
        return true;
    }

    /**
     * Exports measurements using MeasurementsExporter loaded from config
     */
    private static void exportMeasurements( Map<String, String> properties, int opcount, long runtime )
            throws IOException
    {
        MeasurementsExporter exporter = null;
        try
        {
            String exportFilePath = properties.get( EXPORT_FILE_PATH );
            OutputStream out = ( exportFilePath == null ) ? System.out : new FileOutputStream( exportFilePath );
            String exporterClassName = MapUtils.mapGetDefault( properties, EXPORTER,
                    TextMeasurementsExporter.class.getName() );
            try
            {
                exporter = (MeasurementsExporter) Class.forName( exporterClassName ).getConstructor( OutputStream.class ).newInstance(
                        out );
            }
            catch ( Exception e )
            {
                System.err.println( "Could not find exporter " + exporterClassName
                                    + ", will use default text reporter." );
                e.printStackTrace();
                exporter = new TextMeasurementsExporter( out );
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
                exporter.close();
            }
        }
    }

    public static void main( String[] args ) throws IOException
    {
        final long seed = System.currentTimeMillis();
        final RandomDataGeneratorFactory randomFactory = new RandomDataGeneratorFactory( seed );
        final GeneratorBuilder generatorBuilder = new GeneratorBuilder( randomFactory );

        final DbFactory2 dbFactory = new DbFactory2();

        String dbName;

        Map<String, String> commandlineProperties = new HashMap<String, String>();

        Properties fileProperties = new Properties();

        BenchmarkPhase2 argBenchmarkPhase = BenchmarkPhase2.TRANSACTION_PHASE;
        // TODO thread-related, remove
        // int threadCount = 1;
        int target = 0;
        boolean argStatus = false;
        String argLabel;

        // parse arguments
        int argIndex = 0;

        if ( args.length == 0 )
        {
            exit( usageMessage(), 0 );
        }

        while ( args[argIndex].startsWith( "-" ) )
        {
            if ( args[argIndex].equals( "-threads" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    exit( usageMessage(), 0 );
                }
                // TODO thread-related, remove
                // int argThreadCount = Integer.parseInt( args[argIndex] );
                // commandlineProperties.put( "threadcount", Integer.toString(
                // argThreadCount ) );

                argIndex++;
            }
            else if ( args[argIndex].equals( "-target" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    exit( usageMessage(), 0 );
                }
                int argTarget = Integer.parseInt( args[argIndex] );

                commandlineProperties.put( "target", Integer.toString( argTarget ) );

                argIndex++;
            }
            else if ( args[argIndex].equals( "-load" ) )
            {
                argBenchmarkPhase = BenchmarkPhase2.LOAD_PHASE;
                argIndex++;
            }
            else if ( args[argIndex].equals( "-t" ) )
            {
                argBenchmarkPhase = BenchmarkPhase2.TRANSACTION_PHASE;
                argIndex++;
            }
            else if ( args[argIndex].equals( "-s" ) )
            {
                argStatus = true;
                argIndex++;
            }
            else if ( args[argIndex].equals( "-db" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    exit( usageMessage(), 0 );
                }
                String argDb = args[argIndex];

                commandlineProperties.put( "db", argDb );

                argIndex++;
            }
            else if ( args[argIndex].equals( "-l" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    exit( usageMessage(), 0 );
                }
                argLabel = args[argIndex];
                argIndex++;
            }
            else if ( args[argIndex].equals( "-P" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    exit( usageMessage(), 0 );
                }
                String argPropertiesFile = args[argIndex];
                argIndex++;

                fileProperties.load( new FileInputStream( argPropertiesFile ) );
            }
            else if ( args[argIndex].equals( "-p" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    exit( usageMessage(), 0 );
                }
                int equalsCharPosition = args[argIndex].indexOf( '=' );
                if ( equalsCharPosition < 0 )
                {
                    exit( usageMessage(), 0 );
                }

                String argPropertyName = args[argIndex].substring( 0, equalsCharPosition );
                String argPropertyValue = args[argIndex].substring( equalsCharPosition + 1 );
                commandlineProperties.put( argPropertyName, argPropertyValue );
                argIndex++;
            }
            else
            {
                System.out.println( "Unknown option " + args[argIndex] );
                exit( usageMessage(), 0 );
            }

            if ( argIndex >= args.length )
            {
                break;
            }
        }

        if ( argIndex != args.length )
        {
            exit( usageMessage(), 0 );
        }

        // TODO set up logging
        // BasicConfigurator.configure();

        commandlineProperties = MapUtils.mergePropertiesToMap( fileProperties, commandlineProperties, false );

        if ( !checkRequiredProperties( commandlineProperties, requiredProperties ) )
        {
            // TODO there should be a message here
            exit( "", 0 );
        }

        // TODO thread-related, remove
        // get number of threads, target and db
        // threadCount = Integer.parseInt( MapUtils.mapGetDefault(
        // commandlineProperties, "threadcount", "1" ) );

        dbName = MapUtils.mapGetDefault( commandlineProperties, "db", "com.yahoo.ycsb.BasicDB" );

        target = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, "target", "0" ) );

        // compute the target throughput
        double targetPerformancePerMs = -1;
        if ( target > 0 )
        {
            // TODO thread-related, re-calculate (default to one thread?)
            double targetPerThreadPerS = ( (double) target ) / ( 1d );
            // double targetPerThreadPerS = ( (double) target ) / ( (double)
            // threadCount );
            targetPerformancePerMs = targetPerThreadPerS / 1000.0;
        }

        System.out.println( "YCSB Client 0.1" );
        System.out.print( "Command line:" );
        for ( int i = 0; i < args.length; i++ )
        {
            System.out.print( " " + args[i] );
        }
        System.out.println();
        System.err.println( "Loading workload..." );

        // TODO thread-related, consider removing
        // show a warning message that creating the workload is taking a while
        // but only do so if it is taking longer than 2 seconds
        // (showing the message right away if the setup wasn't taking very long
        // was confusing people)
        Thread warningThread = new Thread()
        {
            public void run()
            {
                try
                {
                    sleep( 2000 );
                }
                catch ( InterruptedException e )
                {
                    return;
                }
                System.err.println( " (might take a few minutes for large data sets)" );
            }
        };
        warningThread.start();

        // set up measurements
        Measurements.setProperties( commandlineProperties );

        // load the workload
        ClassLoader classLoader = Client2.class.getClassLoader();

        Workload2 workload = null;

        try
        {
            String workloadClassName = commandlineProperties.get( WORKLOAD );
            Class<? extends Workload2> workloadClass = (Class<? extends Workload2>) classLoader.loadClass( workloadClassName );
            workload = workloadClass.getConstructor( Map.class ).newInstance( commandlineProperties );
        }
        catch ( Exception e )
        {
            // TODO get error msg and print it, rethrow, handle, or something...
            e.printStackTrace();
            e.printStackTrace( System.out );
            // TODO should be message here
            exit( "", 0 );
        }

        warningThread.interrupt();

        // run the workload

        System.err.println( "Starting Benchmark" );

        int operationCount = 0;

        switch ( argBenchmarkPhase )
        {
        case TRANSACTION_PHASE:
            operationCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, OPERATION_COUNT, "0" ) );
            break;

        case LOAD_PHASE:
            if ( commandlineProperties.containsKey( INSERT_COUNT ) )
            {
                operationCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, INSERT_COUNT, "0" ) );
            }
            else
            {
                operationCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, RECORD_COUNT, "0" ) );
            }
            break;
        }

        // TODO thread-related, remove
        Vector<ClientThread2> clientThreads = new Vector<ClientThread2>();

        for ( int threadId = 0; threadId < threadCount; threadId++ )
        {
            Db2 db = null;
            try
            {
                db = dbFactory.newDb( dbName, commandlineProperties );
            }
            catch ( UnknownDBException2 e )
            {
                exit( "Unknown DB " + dbName, 0 );
            }

            // TODO multiple ClientThreads SHARE a Workload instance? Why?
            // TODO should I make it as difficult to start multiple threads on
            // TODO machine as multiple threads on multiple machines
            // TODO REMOVE threading from this layer ENTIRELY
            // TODO move threading to lowest level, per operation
            ClientThread2 clientThread = new ClientThread2( db, argBenchmarkPhase, workload, commandlineProperties,
                    operationCount / threadCount, targetPerformancePerMs, randomFactory.newRandom(), generatorBuilder );

            clientThreads.add( clientThread );
        }

        if ( argStatus )
        {
            // TODO in non-threaded manner
            // boolean standardstatus = false;
            // if ( MapUtils.mapGetDefault( commandlineProperties,
            // "measurementtype", "" ).equals( "timeseries" ) )
            // {
            // standardstatus = true;
            // }
            // statusThread = new StatusThread( clientThreads, argLabel,
            // standardstatus );
            // statusThread.start();
        }

        long st = System.currentTimeMillis();

        for ( ClientThread2 clientThread : clientThreads )
        {
            clientThread.start();
        }

        int opsDone = 0;

        for ( Thread t : clientThreads )
        {
            try
            {
                t.join();
                opsDone += ( (ClientThread2) t ).getOpsDone();
            }
            catch ( InterruptedException e )
            {
            }
        }

        long en = System.currentTimeMillis();

        // TODO in non-threaded manner
        // if ( argStatus )
        // {
        // statusThread.interrupt();
        // }

        // TODO new workload class doesn't have .cleanup(), is it necessary?
        // try
        // {
        // workload.cleanup();
        // }
        // catch ( WorkloadException2 e )
        // {
        // e.printStackTrace();
        // e.printStackTrace( System.out );
        // exit();
        // }

        try
        {
            exportMeasurements( commandlineProperties, opsDone, en - st );
        }
        catch ( IOException e )
        {
            System.err.println( "Could not export measurements, error: " + e.getMessage() );
            e.printStackTrace();
            exit( "", -1 );
        }

        // TODO what is this for?
        exit( "", 0 );
    }

    private static void exit( String message, int errorCode )
    {
        System.out.println( message );
        System.exit( 0 );
    }
}
