package com.ldbc.driver;

import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Properties;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.ldbc.driver.util.MapUtils;

public class WorkloadParams
{
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
    private static final String OPERATION_COUNT_ARG = "oc";
    private static final String OPERATION_COUNT_ARG_LONG = "operationcount";
    private static final String OPERATION_COUNT_DEFAULT = Integer.toString( 0 );
    private static final String OPERATION_COUNT_DESCRIPTION = String.format(
            "number of operations to execute (default: %s)", OPERATION_COUNT_DEFAULT );
    public static final long UNBOUNDED_OPERATION_COUNT = -1;

    private static final String RECORD_COUNT_ARG = "rc";
    private static final String RECORD_COUNT_ARG_LONG = "recordcount";
    private static final String RECORD_COUNT_DEFAULT = Integer.toString( 0 );
    private static final String RECORD_COUNT_DESCRIPTION = String.format(
            "number of records to create during load phase (default: %s)", RECORD_COUNT_DEFAULT );

    // --- REQUIRED ---
    private static final String WORKLOAD_ARG = "w";
    private static final String WORKLOAD_ARG_LONG = "workload";
    private static final String WORKLOAD_EXAMPLE = com.ldbc.driver.workloads.simple.SimpleWorkload.class.getName();
    private static final String WORKLOAD_DESCRIPTION = String.format( "classname of the Workload to use (e.g. %s)",
            WORKLOAD_EXAMPLE );

    private static final String DB_ARG = "db";
    private static final String DB_ARG_LONG = "database";
    private static final String DB_EXAMPLE = com.ldbc.driver.db.basic.BasicDb.class.getName();
    private static final String DB_DESCRIPTION = String.format( "classname of the DB to use (e.g. %s)", DB_EXAMPLE );

    // --- OPTIONAL ---
    private static final String THREADS_ARG = "tc";
    private static final String THREADS_ARG_LONG = "threadcount";
    private static final String THREADS_DEFAULT = Integer.toString( calculateDefaultThreadPoolSize() );
    private static final String THREADS_DESCRIPTION = String.format(
            "number of worker threads to execute with (default: %s)", THREADS_DEFAULT );

    public static int calculateDefaultThreadPoolSize()
    {
        // Client & OperationResultLoggingThread
        int threadsUsedByDriver = 2;
        int totalProcessors = Runtime.getRuntime().availableProcessors();
        int availableProcessors = totalProcessors - threadsUsedByDriver;
        return Math.max( 1, availableProcessors );
    }

    private static final String BENCHMARK_PHASE_ARG = "bp";
    private static final String BENCHMARK_PHASE_LOAD = "l";
    private static final String BENCHMARK_PHASE_LOAD_LONG = "load";
    private static final String BENCHMARK_PHASE_LOAD_DESCRIPTION = "run the loading phase of the workload";
    private static final String BENCHMARK_PHASE_TRANSACTION = "t";
    private static final String BENCHMARK_PHASE_TRANSACTION_LONG = "transaction";
    private static final String BENCHMARK_PHASE_TRANSACTION_DESCRIPTION = "run the transactions phase of the workload";

    private static final String SHOW_STATUS_ARG = "s";
    private static final String SHOW_STATUS_ARG_LONG = "status";
    private static final String SHOW_STATUS_DESCRIPTION = "show status during run";

    private static final String PROPERTY_FILE_ARG = "P";
    private static final String PROPERTY_FILE_DESCRIPTION = "load properties from file(s) - files will be loaded in the order provided";

    private static final String PROPERTY_ARG = "p";
    private static final String PROPERTY_DESCRIPTION = "properties to be passed to DB and Workload - these will override properties loaded from files";

    private static final String TIME_UNIT_ARG = "tu";
    private static final String TIME_UNIT_ARG_LONG = "timeunit";
    private static final String TIME_UNIT_DEFAULT = TimeUnit.MILLISECONDS.toString();
    private static final TimeUnit[] VALID_TIME_UNITS = new TimeUnit[] { TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS,
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES };
    private static final String TIME_UNIT_DESCRIPTION = String.format(
            "time unit to use when gathering metrics. default:%s, valid:%s", TIME_UNIT_DEFAULT,
            Arrays.toString( VALID_TIME_UNITS ) );

    private static final Options OPTIONS = buildOptions();

    public static WorkloadParams fromArgs( String[] args ) throws ParamsException
    {
        Map<String, String> paramsMap;
        try
        {
            paramsMap = parseArgs( args, OPTIONS );
            assertRequiredArgsProvided( paramsMap );
            assertValidTimeUnit( paramsMap.get( TIME_UNIT_ARG ) );
        }
        catch ( ParseException e )
        {
            throw new ParamsException( e.getMessage() );
        }
        catch ( ParamsException e )
        {
            throw new ParamsException( String.format( "%s\n%s", e.getMessage(), helpString() ) );
        }

        /*
         * TODO
         * operation count appears to be null here when it's not set
         * this should not happen, as an exception should be thrown further up during the assert
         * is the assert failing? it is checking for key present, rather than key & not null
         * TODO
         * change has been made to assertRequiredArgsProvided, to check for null value, test it
         */
        String dbClassName = paramsMap.get( DB_ARG );
        String workloadClassName = paramsMap.get( WORKLOAD_ARG );
        long operationCount = Long.parseLong( paramsMap.get( OPERATION_COUNT_ARG ) );
        long recordCount = Long.parseLong( paramsMap.get( RECORD_COUNT_ARG ) );
        BenchmarkPhase benchmarkPhase = BenchmarkPhase.valueOf( paramsMap.get( BENCHMARK_PHASE_ARG ) );
        int threadCount = Integer.parseInt( paramsMap.get( THREADS_ARG ) );
        boolean showStatus = Boolean.parseBoolean( paramsMap.get( SHOW_STATUS_ARG ) );
        TimeUnit timeUnit = TimeUnit.valueOf( paramsMap.get( TIME_UNIT_ARG ) );
        return new WorkloadParams( paramsMap, dbClassName, workloadClassName, operationCount, recordCount,
                benchmarkPhase, threadCount, showStatus, timeUnit );
    }

    private static void assertRequiredArgsProvided( Map<String, String> paramsMap ) throws ParamsException
    {
        List<String> missingOptions = new ArrayList<String>();
        String errMsg = "Missing required option: ";
        if ( false == argIsProvided( paramsMap, DB_ARG ) ) missingOptions.add( DB_ARG );
        if ( false == argIsProvided( paramsMap, WORKLOAD_ARG ) ) missingOptions.add( WORKLOAD_ARG );
        if ( false == argIsProvided( paramsMap, OPERATION_COUNT_ARG ) ) missingOptions.add( OPERATION_COUNT_ARG );
        if ( false == argIsProvided( paramsMap, RECORD_COUNT_ARG ) ) missingOptions.add( RECORD_COUNT_ARG );
        if ( false == argIsProvided( paramsMap, BENCHMARK_PHASE_ARG ) ) missingOptions.add( BENCHMARK_PHASE_ARG );
        if ( false == missingOptions.isEmpty() ) throw new ParamsException( errMsg + missingOptions.toString() );
    }

    private static boolean argIsProvided( Map<String, String> params, String arg )
    {
        if ( false == params.containsKey( arg ) ) return false;
        if ( null == params.get( arg ) ) return false;
        return true;
    }

    private static void assertValidTimeUnit( String timeUnitString ) throws ParamsException
    {
        try
        {
            TimeUnit timeUnit = TimeUnit.valueOf( timeUnitString );
            Set<TimeUnit> validTimeUnits = new HashSet<TimeUnit>();
            validTimeUnits.addAll( Arrays.asList( VALID_TIME_UNITS ) );
            if ( false == validTimeUnits.contains( timeUnit ) )
            {
                throw new IllegalArgumentException();
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new ParamsException( String.format( "Unsupported TimeUnit value: %s", timeUnitString ) );
        }
    }

    private static Map<String, String> parseArgs( String[] args, Options options ) throws ParseException
    {
        Map<String, String> cmdParams = new HashMap<String, String>();
        Map<String, String> fileParams = new HashMap<String, String>();

        CommandLineParser parser = new BasicParser();

        CommandLine cmd = parser.parse( options, args );

        /*
         * Required
         */
        cmdParams.put( DB_ARG, cmd.getOptionValue( DB_ARG ) );

        cmdParams.put( WORKLOAD_ARG, cmd.getOptionValue( WORKLOAD_ARG ) );

        cmdParams.put( OPERATION_COUNT_ARG, cmd.getOptionValue( OPERATION_COUNT_ARG ) );

        cmdParams.put( RECORD_COUNT_ARG, cmd.getOptionValue( RECORD_COUNT_ARG ) );

        BenchmarkPhase phase = ( cmd.hasOption( BENCHMARK_PHASE_LOAD ) ) ? BenchmarkPhase.LOAD_PHASE
                : BenchmarkPhase.TRANSACTION_PHASE;
        cmdParams.put( BENCHMARK_PHASE_ARG, phase.toString() );

        /*
         * Optional
         */
        String threadCount = ( cmd.hasOption( THREADS_ARG ) ) ? cmd.getOptionValue( THREADS_ARG ) : THREADS_DEFAULT;
        cmdParams.put( THREADS_ARG, threadCount );

        cmdParams.put( SHOW_STATUS_ARG, Boolean.toString( cmd.hasOption( SHOW_STATUS_ARG ) ) );

        String timeUnit = ( cmd.hasOption( TIME_UNIT_ARG ) ) ? cmd.getOptionValue( TIME_UNIT_ARG ) : TIME_UNIT_DEFAULT;
        cmdParams.put( TIME_UNIT_ARG, timeUnit );

        if ( cmd.hasOption( PROPERTY_FILE_ARG ) )
        {
            for ( String propertyFilePath : cmd.getOptionValues( PROPERTY_FILE_ARG ) )
            {
                try
                {
                    Properties tempFileProperties = new Properties();
                    tempFileProperties.load( new FileInputStream( propertyFilePath ) );
                    fileParams = MapUtils.mergePropertiesToMap( tempFileProperties, fileParams, true );
                }
                catch ( IOException e )
                {
                    throw new ParseException( String.format( "Error loading properties file %s\n%s", propertyFilePath,
                            e.getMessage() ) );
                }
            }
        }

        if ( cmd.hasOption( PROPERTY_ARG ) )
        {
            for ( Entry<Object, Object> cmdProperty : cmd.getOptionProperties( PROPERTY_ARG ).entrySet() )
            {
                cmdParams.put( (String) cmdProperty.getKey(), (String) cmdProperty.getValue() );
            }
        }

        return convertLongKeysToShortKeys( MapUtils.mergeMaps( fileParams, cmdParams, false ) );
    }

    private static Map<String, String> convertLongKeysToShortKeys( Map<String, String> paramsMap )
    {
        paramsMap = replaceKey( paramsMap, OPERATION_COUNT_ARG_LONG, OPERATION_COUNT_ARG );
        paramsMap = replaceKey( paramsMap, RECORD_COUNT_ARG_LONG, RECORD_COUNT_ARG );
        paramsMap = replaceKey( paramsMap, WORKLOAD_ARG_LONG, WORKLOAD_ARG );
        paramsMap = replaceKey( paramsMap, DB_ARG_LONG, DB_ARG );
        paramsMap = replaceKey( paramsMap, THREADS_ARG_LONG, THREADS_ARG );
        paramsMap = replaceKey( paramsMap, SHOW_STATUS_ARG_LONG, SHOW_STATUS_ARG );
        paramsMap = replaceKey( paramsMap, TIME_UNIT_ARG_LONG, TIME_UNIT_ARG );
        paramsMap = replaceKey( paramsMap, BENCHMARK_PHASE_LOAD_LONG, BENCHMARK_PHASE_LOAD );
        paramsMap = replaceKey( paramsMap, BENCHMARK_PHASE_TRANSACTION_LONG, BENCHMARK_PHASE_TRANSACTION );
        return paramsMap;
    }

    // NOTE: not safe in general case, no check for duplicate keys
    private static Map<String, String> replaceKey( Map<String, String> paramsMap, String oldKey, String newKey )
    {
        if ( false == paramsMap.containsKey( oldKey ) ) return paramsMap;
        String value = paramsMap.get( oldKey );
        paramsMap.remove( oldKey );
        paramsMap.put( newKey, value );
        return paramsMap;
    }

    private static Options buildOptions()
    {
        Options options = new Options();

        /*
         * Required
         */
        // TODO .isRequired() not sufficient as param may come from file
        Option dbOption = OptionBuilder.hasArgs( 1 ).withArgName( "classname" ).withDescription( DB_DESCRIPTION ).withLongOpt(
                DB_ARG_LONG ).create( DB_ARG );
        options.addOption( dbOption );

        Option workloadOption = OptionBuilder.hasArgs( 1 ).withArgName( "classname" ).withDescription(
                WORKLOAD_DESCRIPTION ).withLongOpt( WORKLOAD_ARG_LONG ).create( WORKLOAD_ARG );
        options.addOption( workloadOption );

        Option operationCountOption = OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription(
                OPERATION_COUNT_DESCRIPTION ).withLongOpt( OPERATION_COUNT_ARG_LONG ).create( OPERATION_COUNT_ARG );
        options.addOption( operationCountOption );

        Option recordCountOption = OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription(
                RECORD_COUNT_DESCRIPTION ).withLongOpt( RECORD_COUNT_ARG_LONG ).create( RECORD_COUNT_ARG );
        options.addOption( recordCountOption );

        // TODO .setRequired( true ) not sufficient as param may come from file
        OptionGroup benchmarkPhaseGroup = new OptionGroup();
        Option doLoadOption = OptionBuilder.withDescription( BENCHMARK_PHASE_LOAD_DESCRIPTION ).withLongOpt(
                BENCHMARK_PHASE_LOAD_LONG ).create( BENCHMARK_PHASE_LOAD );
        Option doTransactionsOption = OptionBuilder.withDescription( BENCHMARK_PHASE_TRANSACTION_DESCRIPTION ).withLongOpt(
                BENCHMARK_PHASE_TRANSACTION_LONG ).create( BENCHMARK_PHASE_TRANSACTION );
        benchmarkPhaseGroup.addOption( doLoadOption );
        benchmarkPhaseGroup.addOption( doTransactionsOption );
        options.addOptionGroup( benchmarkPhaseGroup );

        /*
         * Optional
         */
        Option threadsOption = OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription( THREADS_DESCRIPTION ).withLongOpt(
                THREADS_ARG_LONG ).create( THREADS_ARG );
        options.addOption( threadsOption );

        Option statusOption = OptionBuilder.withDescription( SHOW_STATUS_DESCRIPTION ).withLongOpt(
                SHOW_STATUS_ARG_LONG ).create( SHOW_STATUS_ARG );
        options.addOption( statusOption );

        Option timeUnitOption = OptionBuilder.hasArgs( 1 ).withArgName( "unit" ).withDescription( TIME_UNIT_DESCRIPTION ).withLongOpt(
                TIME_UNIT_ARG_LONG ).create( TIME_UNIT_ARG );
        options.addOption( timeUnitOption );

        Option propertyFileOption = OptionBuilder.hasArgs().withValueSeparator( ':' ).withArgName( "file1:file2" ).withDescription(
                PROPERTY_FILE_DESCRIPTION ).create( PROPERTY_FILE_ARG );
        options.addOption( propertyFileOption );

        Option propertyOption = OptionBuilder.hasArgs( 2 ).withValueSeparator( '=' ).withArgName( "key=value" ).withDescription(
                PROPERTY_DESCRIPTION ).create( PROPERTY_ARG );
        options.addOption( propertyOption );

        return options;
    }

    public static String helpString()
    {
        Options options = OPTIONS;
        int printedRowWidth = 110;
        String header = "";
        String footer = "";
        int spacesBeforeOption = 3;
        int spacesBeforeOptionDescription = 5;
        boolean displayUsage = true;
        String commandLineSyntax = "java -cp core-0.1-SNAPSHOT.jar " + Client.class.getName();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter( os );
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp( writer, printedRowWidth, commandLineSyntax, header, options, spacesBeforeOption,
                spacesBeforeOptionDescription, footer, displayUsage );
        writer.flush();
        writer.close();
        return os.toString();
    }

    private final Map<String, String> paramsMap;
    private final String dbClassName;
    private final String workloadClassName;
    private final long operationCount;
    private final long recordCount;
    private final BenchmarkPhase benchmarkPhase;
    private final int threadCount;
    private final boolean showStatus;
    private final TimeUnit timeUnit;

    public WorkloadParams( Map<String, String> paramsMap, String dbClassName, String workloadClassName,
            long operationCount, long recordCount, BenchmarkPhase benchmarkPhase, int threadCount, boolean showStatus,
            TimeUnit timeUnit )
    {
        this.paramsMap = paramsMap;
        this.dbClassName = dbClassName;
        this.workloadClassName = workloadClassName;
        this.operationCount = operationCount;
        this.recordCount = recordCount;
        this.benchmarkPhase = benchmarkPhase;
        this.threadCount = threadCount;
        this.showStatus = showStatus;
        this.timeUnit = timeUnit;
    }

    public String getDbClassName()
    {
        return dbClassName;
    }

    public String getWorkloadClassName()
    {
        return workloadClassName;
    }

    public long getOperationCount()
    {
        return operationCount;
    }

    public long getRecordCount()
    {
        return recordCount;
    }

    public BenchmarkPhase getBenchmarkPhase()
    {
        return benchmarkPhase;
    }

    public int getThreadCount()
    {
        return threadCount;
    }

    public boolean isShowStatus()
    {
        return showStatus;
    }

    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    public Map<String, String> asMap()
    {
        return paramsMap;
    }

    public String getHelpString()
    {
        return helpString();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "Parameters:" ).append( "\n" );
        sb.append( "\t" ).append( "DB:\t\t\t" ).append( dbClassName ).append( "\n" );
        sb.append( "\t" ).append( "Workload:\t\t" ).append( workloadClassName ).append( "\n" );
        sb.append( "\t" ).append( "Operation Count:\t" ).append( operationCount ).append( "\n" );
        sb.append( "\t" ).append( "Record Count:\t\t" ).append( recordCount ).append( "\n" );
        sb.append( "\t" ).append( "Benchmark Phase:\t" ).append( benchmarkPhase ).append( "\n" );
        sb.append( "\t" ).append( "Worker Threads:\t\t" ).append( threadCount ).append( "\n" );
        sb.append( "\t" ).append( "Show Status:\t\t" ).append( showStatus ).append( "\n" );
        sb.append( "\t" ).append( "Time Unit:\t\t" ).append( timeUnit ).append( "\n" );

        Set<String> excludedKeys = new HashSet<String>();
        excludedKeys.addAll( Arrays.asList( new String[] { DB_ARG, WORKLOAD_ARG, OPERATION_COUNT_ARG, RECORD_COUNT_ARG,
                BENCHMARK_PHASE_ARG, THREADS_ARG, SHOW_STATUS_ARG, TIME_UNIT_ARG } ) );
        Map<String, String> filteredParamsMap = MapUtils.copyExcludingKeys( paramsMap, excludedKeys );
        if ( false == filteredParamsMap.isEmpty() )
        {
            sb.append( "\t" ).append( "User-defined parameters:" ).append( "\n" );
            sb.append( MapUtils.prettyPrint( filteredParamsMap, "\t\t" ) );
        }

        return sb.toString();
    }
}
