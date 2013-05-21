/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package OLD_com.ldbc;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import OLD_com.ldbc.workloads.Workload;
import OLD_com.ldbc.workloads.WorkloadException;

import com.ldbc.generator.GeneratorBuilderFactory;
import com.ldbc.measurements.Measurements;
import com.ldbc.measurements.MeasurementsException;
import com.ldbc.measurements.exporter.MeasurementsExporter;
import com.ldbc.measurements.exporter.TextMeasurementsExporter;
import com.ldbc.util.RandomDataGeneratorFactory;
import com.ldbc.util.MapUtils;

/**
 * Main class for executing YCSB
 */
public class Client
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

    /**
     * Maximum time (seconds) the benchmark will be run
     */
    public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

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
        System.out.println( "  -s:  show status during run (default: no status)" );
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
     * Exports measurements to either sysout or file, using the exporter loaded
     * from configuration
     * 
     * @throws IOException Either failed to write to output stream or failed to
     *             close it.
     * @throws MeasurementsException
     */
    private static void exportMeasurements( Map<String, String> properties, int opcount, long runtime )
            throws IOException, MeasurementsException
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
        final GeneratorBuilderFactory abstractGeneratorFactory = new GeneratorBuilderFactory( randomFactory.newRandom() );

        final DBFactory dbFactory = new DBFactory();

        String dbName;

        Map<String, String> commandlineProperties = new HashMap<String, String>();

        Properties fileProperties = new Properties();

        BenchmarkPhase argBenchmarkPhase = BenchmarkPhase.TRANSACTION_PHASE;
        int threadCount = 1;
        int target = 0;
        boolean argStatus = false;
        String argLabel = "";

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
                argBenchmarkPhase = BenchmarkPhase.LOAD_PHASE;
                argIndex++;
            }
            else if ( args[argIndex].equals( "-t" ) )
            {
                argBenchmarkPhase = BenchmarkPhase.TRANSACTION_PHASE;
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

        // TODO change to milliseconds instead of seconds
        long maxExecutionTime = Long.parseLong( MapUtils.mapGetDefault( commandlineProperties, MAX_EXECUTION_TIME, "0" ) );

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
        Thread warningthread = new Thread()
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

        warningthread.start();

        // set up measurements
        Measurements.setProperties( commandlineProperties );

        // load the workload
        ClassLoader classLoader = Client.class.getClassLoader();

        Workload workload = null;

        try
        {
            String workloadClassName = commandlineProperties.get( WORKLOAD );
            Class<? extends Workload> workloadclass = (Class<? extends Workload>) classLoader.loadClass( workloadClassName );
            workload = workloadclass.newInstance();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            e.printStackTrace( System.out );
            System.exit( 0 );
        }

        try
        {
            workload.init( commandlineProperties, abstractGeneratorFactory.newGeneratorBuilder() );
        }
        catch ( WorkloadException e )
        {
            e.printStackTrace();
            e.printStackTrace( System.out );
            System.exit( 0 );
        }

        warningthread.interrupt();

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

        Vector<ClientThread> clientThreads = new Vector<ClientThread>();

        for ( int threadId = 0; threadId < threadCount; threadId++ )
        {
            DB db = null;
            try
            {
                db = dbFactory.newDB( dbName, commandlineProperties );
            }
            catch ( UnknownDBException e )
            {
                System.out.println( "Unknown DB " + dbName );
                System.exit( 0 );
            }

            // TODO multiple ClientThreads SHARE a Workload instance? Why?
            // TODO should I make it as difficult to start multiple threads on
            // TODO machine as multiple threads on multiple machines
            // TODO REMOVE threading from this layer ENTIRELY
            // TODO move threading to lowest level, per operation

            ClientThread clientThread = new ClientThread( db, argBenchmarkPhase, workload, threadId, threadCount,
                    commandlineProperties, operationCount / threadCount, targetPerformancePerMs,
                    randomFactory.newRandom() );

            clientThreads.add( clientThread );
        }

        StatusThread statusThread = null;

        if ( argStatus )
        {
            boolean standardstatus = false;
            if ( MapUtils.mapGetDefault( commandlineProperties, "measurementtype", "" ).equals( "timeseries" ) )
            {
                standardstatus = true;
            }
            statusThread = new StatusThread( clientThreads, argLabel, standardstatus );
            statusThread.start();
        }

        long st = System.currentTimeMillis();

        for ( ClientThread clientThread : clientThreads )
        {
            clientThread.start();
        }

        Thread terminatorThread = null;

        if ( maxExecutionTime > 0 )
        {
            terminatorThread = new TerminatorThread( maxExecutionTime, clientThreads, workload );
            terminatorThread.start();
        }

        int opsDone = 0;

        for ( Thread t : clientThreads )
        {
            try
            {
                t.join();
                opsDone += ( (ClientThread) t ).getOpsDone();
            }
            catch ( InterruptedException e )
            {
            }
        }

        long en = System.currentTimeMillis();

        if ( terminatorThread != null && !terminatorThread.isInterrupted() )
        {
            terminatorThread.interrupt();
        }

        if ( argStatus )
        {
            statusThread.interrupt();
        }

        try
        {
            workload.cleanup();
        }
        catch ( WorkloadException e )
        {
            e.printStackTrace();
            e.printStackTrace( System.out );
            System.exit( 0 );
        }

        try
        {
            exportMeasurements( commandlineProperties, opsDone, en - st );
        }
        catch ( Exception e )
        {
            System.err.println( "Could not export measurements, error: " + e.getMessage() );
            e.printStackTrace();
            System.exit( -1 );
        }

        System.exit( 0 );
    }
}
