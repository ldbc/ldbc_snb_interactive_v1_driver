package com.ldbc2;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
 * Main class for executing YCSB
 */
public class Client2
{
    public static final String OPERATION_COUNT = "operationcount";
    public static final String RECORD_COUNT = "recordcount";
    public static final String WORKLOAD = "workload";

    public static final String EXPORTER = "exporter";
    public static final String EXPORT_FILE_PATH = "exportfile";

    /**
     * For partitioning load among machines when client is bottleneck.
     * INSERT_COUNT specifies number of inserts client should do, if less than
     * RECORD_COUNT. Workloads should support the INSERT_START property, which
     * specifies the record to start at (offset).
     */
    public static final String INSERT_COUNT = "insertcount";

    public static void printUsageMessage()
    {
        System.out.println( "Usage: java com.yahoo.ycsb.Client [options]" );
        System.out.println( "Options:" );
        System.out.println( "  -threads n: execute using n threads (default: 1) - can also be specified as the \n"
                            + "              \"threadcount\" property using -p" );
        System.out.println( "  -target n: attempt to do n operations per second (default: unlimited) - can also\n"
                            + "             be specified as the \"target\" property using -p" );
        System.out.println( "  -load:  run the loading phase of the workload" );
        System.out.println( "  -t:  run the transactions phase of the workload (default)" );
        System.out.println( "  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n"
                            + "              can also be specified as the \"db\" property using -p" );
        System.out.println( "  -P propertyfile: load properties from the given file. Multiple files can" );
        System.out.println( "                   be specified, and will be processed in the order specified" );
        System.out.println( "  -p name=value:  specify a property to be passed to the DB and workloads;" );
        System.out.println( "                  multiple properties can be specified, and override any" );
        System.out.println( "                  values in the propertyfile" );
        System.out.println( "  -l label:  use label for status (e.g. to label one experiment out of a whole batch)" );
        System.out.println( "" );
        System.out.println( "Required properties:" );
        System.out.println( "  " + WORKLOAD
                            + ": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)" );
        System.out.println( "" );
        System.out.println( "To run the transaction phase from multiple servers, start a separate client on each." );
        System.out.println( "To run the load phase from multiple servers, start a separate client on each; additionally," );
        System.out.println( "use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted" );
    }

    public static boolean checkRequiredProperties( Map<String, String> properties )
    {
        if ( false == properties.containsKey( WORKLOAD ) )
        {
            // TODO use logger
            System.out.println( "Missing property: " + WORKLOAD );
            return false;
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
        int threadCount = 1;
        int target = 0;
        String argLabel;

        // parse arguments
        int argIndex = 0;

        if ( args.length == 0 )
        {
            printUsageMessage();
            System.exit( 0 );
        }

        while ( args[argIndex].startsWith( "-" ) )
        {
            if ( args[argIndex].equals( "-threads" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage();
                    System.exit( 0 );
                }
                int argThreadCount = Integer.parseInt( args[argIndex] );

                commandlineProperties.put( "threadcount", Integer.toString( argThreadCount ) );

                argIndex++;
            }
            else if ( args[argIndex].equals( "-target" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage();
                    System.exit( 0 );
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
            else if ( args[argIndex].equals( "-db" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage();
                    System.exit( 0 );
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
                    printUsageMessage();
                    System.exit( 0 );
                }
                argLabel = args[argIndex];
                argIndex++;
            }
            else if ( args[argIndex].equals( "-P" ) )
            {
                argIndex++;
                if ( argIndex >= args.length )
                {
                    printUsageMessage();
                    System.exit( 0 );
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
                    printUsageMessage();
                    System.exit( 0 );
                }
                int equalsCharPosition = args[argIndex].indexOf( '=' );
                if ( equalsCharPosition < 0 )
                {
                    printUsageMessage();
                    System.exit( 0 );
                }

                String argPropertyName = args[argIndex].substring( 0, equalsCharPosition );
                String argPropertyValue = args[argIndex].substring( equalsCharPosition + 1 );
                commandlineProperties.put( argPropertyName, argPropertyValue );
                argIndex++;
            }
            else
            {
                System.out.println( "Unknown option " + args[argIndex] );
                printUsageMessage();
                System.exit( 0 );
            }

            if ( argIndex >= args.length )
            {
                break;
            }
        }

        if ( argIndex != args.length )
        {
            printUsageMessage();
            System.exit( 0 );
        }

        // TODO set up logging
        // BasicConfigurator.configure();

        commandlineProperties = MapUtils.mergePropertiesToMap( fileProperties, commandlineProperties, false );

        if ( !checkRequiredProperties( commandlineProperties ) )
        {
            System.exit( 0 );
        }

        // get number of threads, target and db
        threadCount = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, "threadcount", "1" ) );

        dbName = MapUtils.mapGetDefault( commandlineProperties, "db", "com.yahoo.ycsb.BasicDB" );

        target = Integer.parseInt( MapUtils.mapGetDefault( commandlineProperties, "target", "0" ) );

        // compute the target throughput
        double targetPerformancePerMs = -1;
        if ( target > 0 )
        {
            double targetPerThreadPerS = ( (double) target ) / ( (double) threadCount );
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
            e.printStackTrace();
            e.printStackTrace( System.out );
            System.exit( 0 );
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
                System.out.println( "Unknown DB " + dbName );
                System.exit( 0 );
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

        // TODO new workload class doesn't have .cleanup(), is itnecessary?
        // try
        // {
        // workload.cleanup();
        // }
        // catch ( WorkloadException2 e )
        // {
        // e.printStackTrace();
        // e.printStackTrace( System.out );
        // System.exit( 0 );
        // }

        try
        {
            exportMeasurements( commandlineProperties, opsDone, en - st );
        }
        catch ( IOException e )
        {
            System.err.println( "Could not export measurements, error: " + e.getMessage() );
            e.printStackTrace();
            System.exit( -1 );
        }

        System.exit( 0 );
    }
}
