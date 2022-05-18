package org.ldbcouncil.snb.driver.control;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.ldbcouncil.snb.driver.Client;
import org.ldbcouncil.snb.driver.temporal.TemporalUtil;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveDb;
import org.ldbcouncil.snb.driver.workloads.simple.db.SimpleDb;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ConsoleAndFileDriverConfiguration implements DriverConfiguration
{
    private static final TemporalUtil TEMPORAL_UTIL = new TemporalUtil();
    private static final DecimalFormat INTEGRAL_FORMAT = new DecimalFormat( "###,###,###,###,###" );
    private static final DecimalFormat FLOAT_FORMAT = new DecimalFormat( "###,###,###,###,##0.0000000" );

    // --- MODE ---
    public static final String MODE_ARG = "om";
    public static final String MODE_DEFAULT = "execute_benchmark";
    public static final String MODE_DEFAULT_STRING = MODE_DEFAULT;
    private static final String MODE_ARG_LONG = "mode";
    private static final String MODE_DESCRIPTION = 
        "mode the driver should execute (e.g. create_validation, validate_database, create_statistics, execute_benchmark)";

    // --- REQUIRED ---
    public static final String OPERATION_COUNT_ARG = "oc";
    public static final long OPERATION_COUNT_DEFAULT = 0;
    public static final String OPERATION_COUNT_DEFAULT_STRING = Long.toString( OPERATION_COUNT_DEFAULT );
    private static final String OPERATION_COUNT_ARG_LONG = "operation_count";
    private static final String OPERATION_COUNT_DESCRIPTION = "number of operations to execute";

    public static final String WORKLOAD_ARG = "w";
    private static final String WORKLOAD_ARG_LONG = "workload";
    public static final String WORKLOAD_DEFAULT = null;
    public static final String WORKLOAD_DEFAULT_STRING = WORKLOAD_DEFAULT;
    private static final String WORKLOAD_EXAMPLE = org.ldbcouncil.snb.driver.workloads.simple.SimpleWorkload.class.getName();
    private static final String WORKLOAD_DESCRIPTION =
            format( "class name of the Workload to use (e.g. %s)", WORKLOAD_EXAMPLE );

    public static final String DB_ARG = "db";
    private static final String DB_ARG_LONG = "database";
    public static final String DB_DEFAULT = null;
    public static final String DB_DEFAULT_STRING = DB_DEFAULT;
    private static final String DB_EXAMPLE = SimpleDb.class.getName();
    private static final String DB_DESCRIPTION = format( "class name of the DB to use (e.g. %s)", DB_EXAMPLE );

    // --- OPTIONAL ---
    public static final String IGNORE_SCHEDULED_START_TIMES_ARG = "ignore_scheduled_start_times";
    public static final boolean IGNORE_SCHEDULED_START_TIMES_DEFAULT = false;
    public static final String IGNORE_SCHEDULED_START_TIMES_DEFAULT_STRING =
            Boolean.toString( IGNORE_SCHEDULED_START_TIMES_DEFAULT );
    private static final String IGNORE_SCHEDULED_START_TIMES_DESCRIPTION =
            "executes operations as fast as possible, ignoring their scheduled start times";

    public static final String HELP_ARG = "help";
    public static final boolean HELP_DEFAULT = false;
    public static final String HELP_DEFAULT_STRING = Boolean.toString( HELP_DEFAULT );
    private static final String HELP_DESCRIPTION = "print usage instruction";

    public static final String FLUSH_LOG_ARG = "flush_log";
    public static final boolean FLUSH_LOG_DEFAULT = false;
    public static final String FLUSH_LOG_DEFAULT_STRING = Boolean.toString(FLUSH_LOG_DEFAULT);
    private static final String FLUSH_LOG_DESCRIPTION = "flush log to disk after each operation";

    public static final String NAME_ARG = "nm";
    private static final String NAME_ARG_LONG = "name";
    public static final String NAME_DEFAULT = "LDBC";
    public static final String NAME_DEFAULT_STRING = NAME_DEFAULT;
    private static final String NAME_DESCRIPTION =
            format( "name of the benchmark run. default = %s", NAME_DEFAULT );

    public static final String RESULT_DIR_PATH_ARG = "rd";
    private static final String RESULT_DIR_PATH_ARG_LONG = "results_dir";
    public static final String RESULT_DIR_PATH_DEFAULT = "results";
    public static final String RESULT_DIR_PATH_DEFAULT_STRING = RESULT_DIR_PATH_DEFAULT;
    private static final String RESULT_DIR_PATH_DESCRIPTION =
            format( "directory where benchmark results will be written. default = %s", RESULT_DIR_PATH_DEFAULT );

    public static final String THREADS_ARG = "tc";
    private static final String THREADS_ARG_LONG = "thread_count";
    public static final int THREADS_DEFAULT = 1;
    public static final String THREADS_DEFAULT_STRING = Integer.toString( THREADS_DEFAULT );
    private static final String THREADS_DESCRIPTION =
            format( "number of worker threads to execute with (default: %s)", THREADS_DEFAULT_STRING );

    public static final String SHOW_STATUS_ARG = "s";
    private static final String SHOW_STATUS_ARG_LONG = "status";
    public static final int SHOW_STATUS_DEFAULT = 2;
    public static final String SHOW_STATUS_DEFAULT_STRING = Integer.toString( SHOW_STATUS_DEFAULT );
    private static final String SHOW_STATUS_DESCRIPTION =
            "interval between status printouts during benchmark execution (0 = disable)";

    public static final String DB_VALIDATION_FILE_PATH_ARG = "vdb";
    private static final String DB_VALIDATION_FILE_PATH_ARG_LONG = "validate_database";
    public static final String DB_VALIDATION_FILE_PATH_DEFAULT = null;
    public static final String DB_VALIDATION_FILE_PATH_DEFAULT_STRING = DB_VALIDATION_FILE_PATH_DEFAULT;
    private static final String DB_VALIDATION_FILE_PATH_DESCRIPTION =
            "path to validation parameters file, if provided database connector will be validated";

    public static final String VALIDATION_PARAMS_SIZE_ARG = "vps";
    private static final String VALIDATION_PARAMS_SIZE_ARG_LONG = "validation_parameters_size";
    public static final int VALIDATION_PARAMS_SIZE_DEFAULT = 0;
    public static final String VALIDATION_PARAMS_SIZE_DEFAULT_STRING = Integer.toString( VALIDATION_PARAMS_SIZE_DEFAULT );
    private static final String VALIDATION_PARAMS_SIZE_DESCRIPTION =
            "The size of validation set to create.";

    public static final String VALIDATION_SERIALIZATION_CHECK_ARG = "vsc";
    private static final String VALIDATION_SERIALIZATION_CHECK_ARG_LONG = "validation_parameters_serialization_check";
    public static final Boolean VALIDATION_SERIALIZATION_CHECK_DEFAULT = true;
    public static final String VALIDATION_SERIALIZATION_CHECK_DEFAULT_STRING =
    Boolean.toString( VALIDATION_SERIALIZATION_CHECK_DEFAULT );
    private static final String VALIDATION_SERIALIZATION_CHECK_DESCRIPTION =
            "Check serialized queries and results by deserializing them and compare.";

    public static final String TIME_UNIT_ARG = "tu";
    private static final String TIME_UNIT_ARG_LONG = "time_unit";
    public static final TimeUnit TIME_UNIT_DEFAULT = TimeUnit.MILLISECONDS;
    public static final String TIME_UNIT_DEFAULT_STRING = TIME_UNIT_DEFAULT.toString();
    private static final TimeUnit[] VALID_TIME_UNITS = new TimeUnit[]{TimeUnit.NANOSECONDS, TimeUnit.MICROSECONDS,
            TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES};
    private static final String TIME_UNIT_DESCRIPTION = format(
            "time unit to use when gathering metrics. default:%s, valid:%s", TIME_UNIT_DEFAULT_STRING,
            Arrays.toString( VALID_TIME_UNITS ) );

    public static final String TIME_COMPRESSION_RATIO_ARG = "tcr";
    private static final String TIME_COMPRESSION_RATIO_ARG_LONG = "time_compression_ratio";
    public static final double TIME_COMPRESSION_RATIO_DEFAULT = 1.0; // 1.0 == do not compress
    public static final String TIME_COMPRESSION_RATIO_DEFAULT_STRING =
            Double.toString( TIME_COMPRESSION_RATIO_DEFAULT );
    private static final String TIME_COMPRESSION_RATIO_DESCRIPTION = "change duration between operations of workload";

    public static final String SPINNER_SLEEP_DURATION_ARG = "sw";
    private static final String SPINNER_SLEEP_DURATION_ARG_LONG = "spinner_wait_duration";
    public static final long SPINNER_SLEEP_DURATION_DEFAULT = 1;
    public static final String SPINNER_SLEEP_DURATION_DEFAULT_STRING = Long.toString( SPINNER_SLEEP_DURATION_DEFAULT );
    private static final String SPINNER_SLEEP_DURATION_DESCRIPTION =
            "sleep duration (ms) injected into busy wait loops (to reduce CPU consumption)";

    public static final String SKIP_COUNT_ARG = "sk";
    private static final String SKIP_COUNT_ARG_LONG = "skip";
    public static final long SKIP_COUNT_DEFAULT = 0;
    public static final String SKIP_COUNT_DEFAULT_STRING = Long.toString( SKIP_COUNT_DEFAULT );
    private static final String SKIP_COUNT_DESCRIPTION =
            format( "number of operations to skip over before beginning execution (default: %s)",
                    SKIP_COUNT_DEFAULT_STRING );

    public static final String WARMUP_COUNT_ARG = "wu";
    private static final String WARMUP_COUNT_ARG_LONG = "warmup";
    public static final long WARMUP_COUNT_DEFAULT = 0;
    public static final String WARMUP_COUNT_DEFAULT_STRING = Long.toString( WARMUP_COUNT_DEFAULT );
    private static final String WARMUP_COUNT_DESCRIPTION =
            format( "number of operations to execute during warmup phase (default: %s)",
                    WARMUP_COUNT_DEFAULT_STRING );

    public static final String PROPERTY_FILE_ARG = "P";
    private static final String PROPERTY_FILE_DESCRIPTION =
            "load properties from file(s) - files will be loaded in the order provided\n" +
            "first files are highest priority; later values will not override earlier values";

    public static final String PROPERTY_ARG = "p";
    private static final String PROPERTY_DESCRIPTION =
            "properties to be passed to DB and Workload - these will override properties loaded from files";

    private static final Options OPTIONS = buildOptions();

    private static final char COMMANDLINE_SEPARATOR_CHAR = '|';

    public static Map<String,String> defaultsAsMap() throws DriverConfigurationException
    {
        Map<String,String> defaultParamsMap = new HashMap<>();
        defaultParamsMap.put( MODE_ARG, MODE_DEFAULT_STRING);
        defaultParamsMap.put( IGNORE_SCHEDULED_START_TIMES_ARG, IGNORE_SCHEDULED_START_TIMES_DEFAULT_STRING );
        defaultParamsMap.put( HELP_ARG, HELP_DEFAULT_STRING );
        defaultParamsMap.put( FLUSH_LOG_ARG, FLUSH_LOG_DEFAULT_STRING );
        defaultParamsMap.put( OPERATION_COUNT_ARG, OPERATION_COUNT_DEFAULT_STRING );
        defaultParamsMap.put( WORKLOAD_ARG, WORKLOAD_DEFAULT_STRING );
        defaultParamsMap.put( NAME_ARG, NAME_DEFAULT_STRING );
        defaultParamsMap.put( DB_ARG, DB_DEFAULT_STRING );
        defaultParamsMap.put( RESULT_DIR_PATH_ARG, RESULT_DIR_PATH_DEFAULT_STRING );
        defaultParamsMap.put( THREADS_ARG, THREADS_DEFAULT_STRING );
        defaultParamsMap.put( SHOW_STATUS_ARG, SHOW_STATUS_DEFAULT_STRING );
        if ( null != DB_VALIDATION_FILE_PATH_DEFAULT_STRING )
        {
            defaultParamsMap.put( DB_VALIDATION_FILE_PATH_ARG, DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
        }
        defaultParamsMap.put( VALIDATION_SERIALIZATION_CHECK_ARG, VALIDATION_SERIALIZATION_CHECK_DEFAULT_STRING );
        defaultParamsMap.put( VALIDATION_PARAMS_SIZE_ARG, VALIDATION_PARAMS_SIZE_DEFAULT_STRING );
        defaultParamsMap.put( TIME_UNIT_ARG, TIME_UNIT_DEFAULT_STRING );
        defaultParamsMap.put( TIME_COMPRESSION_RATIO_ARG, TIME_COMPRESSION_RATIO_DEFAULT_STRING );
        defaultParamsMap.put( SPINNER_SLEEP_DURATION_ARG, SPINNER_SLEEP_DURATION_DEFAULT_STRING );
        defaultParamsMap.put( WARMUP_COUNT_ARG, WARMUP_COUNT_DEFAULT_STRING );
        defaultParamsMap.put( SKIP_COUNT_ARG, SKIP_COUNT_DEFAULT_STRING );
        return defaultParamsMap;
    }

    public static ConsoleAndFileDriverConfiguration fromArgs( String[] args ) throws DriverConfigurationException
    {
        try
        {
            Map<String,String> paramsMap = parseArgs( args, OPTIONS );
            return fromParamsMap( paramsMap );
        }
        catch ( Exception e )
        {
            throw new DriverConfigurationException( format( "%s\n%s", e.getMessage(), commandlineHelpString() ), e );
        }
    }

    public static List<String> checkMissingParams(DriverConfiguration configuration)
    {
        List<String> missingParams = new ArrayList<>();
        if ( null == configuration.workloadClassName() )
        {
            missingParams.add( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG );
        }
        OperationMode mode = OperationMode.valueOf(configuration.mode());
        switch (mode) {
            case create_statistics:
                if ( 0 == configuration.operationCount() )
                {
                    missingParams.add( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG );
                }
                break;
            case validate_database:
                if ( null == configuration.dbClassName() )
                {
                    missingParams.add( ConsoleAndFileDriverConfiguration.DB_ARG );
                }
                break;
            case create_validation:
            case execute_benchmark:
            default: // Execute benchmark is default behaviour
                if ( null == configuration.dbClassName() )
                {
                    missingParams.add( ConsoleAndFileDriverConfiguration.DB_ARG );
                }
                if ( 0 == configuration.operationCount() )
                {
                    missingParams.add( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG );
                }
                break;
        }
        return missingParams;
    }

    public static ConsoleAndFileDriverConfiguration fromDefaults(
            String databaseClassName,
            String workloadClassName,
            long operationCount ) throws DriverConfigurationException
    {
        try
        {
            Map<String,String> paramsMap = defaultsAsMap();
            paramsMap.put( DB_ARG, databaseClassName );
            paramsMap.put( WORKLOAD_ARG, workloadClassName );
            paramsMap.put( OPERATION_COUNT_ARG, Long.toString( operationCount ) );
            return fromParamsMap( paramsMap );
        }
        catch ( DriverConfigurationException e )
        {
            throw new DriverConfigurationException( format( "%s\n%s", e.getMessage(), commandlineHelpString() ),
                    e );
        }
    }

    public static ConsoleAndFileDriverConfiguration fromParamsMap( Map<String,String> paramsMap )
            throws DriverConfigurationException
    {
        try
        {
            paramsMap = convertLongKeysToShortKeys( paramsMap );

            if ( paramsMap.containsKey( TIME_UNIT_ARG ) )
            {
                assertValidTimeUnit( paramsMap.get( TIME_UNIT_ARG ) );
            }

            paramsMap = MapUtils.mergeMaps( paramsMap, defaultsAsMap(), false );
            String mode = paramsMap.get ( MODE_ARG );
            String name = paramsMap.get( NAME_ARG );
            String dbClassName = paramsMap.get( DB_ARG );
            String workloadClassName = paramsMap.get( WORKLOAD_ARG );
            long operationCount = Long.parseLong( paramsMap.get( OPERATION_COUNT_ARG ) );
            int threadCount = Integer.parseInt( paramsMap.get( THREADS_ARG ) );
            int statusDisplayIntervalAsSeconds = Integer.parseInt( paramsMap.get( SHOW_STATUS_ARG ) );
            TimeUnit timeUnit = TimeUnit.valueOf( paramsMap.get( TIME_UNIT_ARG ) );
            String resultDirPath = paramsMap.get( RESULT_DIR_PATH_ARG );
            double timeCompressionRatio = Double.parseDouble( paramsMap.get( TIME_COMPRESSION_RATIO_ARG ) );
            int validationParametersSize = Integer.parseInt( paramsMap.get( VALIDATION_PARAMS_SIZE_ARG ) );
            boolean validationSerializationCheck =
                Boolean.parseBoolean( paramsMap.get( VALIDATION_SERIALIZATION_CHECK_ARG ) );
            String databaseValidationFilePath = paramsMap.get( DB_VALIDATION_FILE_PATH_ARG );
            long spinnerSleepDurationAsMilli = Long.parseLong( paramsMap.get( SPINNER_SLEEP_DURATION_ARG ) );
            long skipCount = Long.parseLong( paramsMap.get( SKIP_COUNT_ARG ) );
            long warmupCount = Long.parseLong( paramsMap.get( WARMUP_COUNT_ARG ) );
            boolean printHelp = Boolean.parseBoolean( paramsMap.get( HELP_ARG ) );
            boolean ignoreScheduledStartTimes =
                    Boolean.parseBoolean( paramsMap.get( IGNORE_SCHEDULED_START_TIMES_ARG ) );
            boolean flushLog = Boolean.parseBoolean( paramsMap.get( FLUSH_LOG_ARG ) );
            return new ConsoleAndFileDriverConfiguration(
                    paramsMap,
                    mode,
                    name,
                    dbClassName,
                    workloadClassName,
                    operationCount,
                    threadCount,
                    statusDisplayIntervalAsSeconds,
                    timeUnit,
                    resultDirPath,
                    timeCompressionRatio,
                    validationParametersSize,
                    validationSerializationCheck,
                    databaseValidationFilePath,
                    // calculateWorkloadStatistics,
                    spinnerSleepDurationAsMilli,
                    printHelp,
                    ignoreScheduledStartTimes,
                    warmupCount,
                    skipCount,
                    flushLog
            );
        }
        catch ( DriverConfigurationException e )
        {
            throw new DriverConfigurationException( format( "%s\n%s", e.getMessage(), commandlineHelpString() ),
                    e );
        }
    }

    private static void assertValidTimeUnit( String timeUnitString ) throws DriverConfigurationException
    {
        try
        {
            TimeUnit timeUnit = TimeUnit.valueOf( timeUnitString );
            Set<TimeUnit> validTimeUnits = new HashSet<>();
            validTimeUnits.addAll( Arrays.asList( VALID_TIME_UNITS ) );
            if ( !validTimeUnits.contains( timeUnit ) )
            {
                throw new IllegalArgumentException();
            }
        }
        catch ( IllegalArgumentException e )
        {
            throw new DriverConfigurationException( format( "Unsupported TimeUnit value: %s", timeUnitString ) );
        }
    }

    private static Map<String,String> parseArgs( String[] args, Options options )
            throws ParseException, DriverConfigurationException
    {
        Map<String,String> cmdParams = new HashMap<>();
        Map<String,String> fileParams = new HashMap<>();

        CommandLineParser parser = new BasicParser();

        CommandLine cmd = parser.parse( options, args );

        /*
         * Required
         */
        if ( cmd.hasOption( MODE_ARG ) )
        {
            cmdParams.put( MODE_ARG, cmd.getOptionValue( MODE_ARG ) );
        }

        if ( cmd.hasOption( DB_ARG ) )
        {
            cmdParams.put( DB_ARG, cmd.getOptionValue( DB_ARG ) );
        }

        if ( cmd.hasOption( WORKLOAD_ARG ) )
        {
            cmdParams.put( WORKLOAD_ARG, cmd.getOptionValue( WORKLOAD_ARG ) );
        }

        if ( cmd.hasOption( OPERATION_COUNT_ARG ) )
        {
            cmdParams.put( OPERATION_COUNT_ARG, cmd.getOptionValue( OPERATION_COUNT_ARG ) );
        }

        /*
         * Optional
         */
        if ( cmd.hasOption( NAME_ARG ) )
        {
            cmdParams.put( NAME_ARG, cmd.getOptionValue( NAME_ARG ) );
        }

        if ( cmd.hasOption( RESULT_DIR_PATH_ARG ) )
        {
            cmdParams.put( RESULT_DIR_PATH_ARG, cmd.getOptionValue( RESULT_DIR_PATH_ARG ) );
        }

        if ( cmd.hasOption( THREADS_ARG ) )
        {
            cmdParams.put( THREADS_ARG, cmd.getOptionValue( THREADS_ARG ) );
        }

        if ( cmd.hasOption( SHOW_STATUS_ARG ) )
        {
            cmdParams.put( SHOW_STATUS_ARG, cmd.getOptionValue( SHOW_STATUS_ARG ) );
        }

        if ( cmd.hasOption( TIME_UNIT_ARG ) )
        {
            cmdParams.put( TIME_UNIT_ARG, cmd.getOptionValue( TIME_UNIT_ARG ) );
        }

        if ( cmd.hasOption( TIME_COMPRESSION_RATIO_ARG ) )
        {
            cmdParams.put( TIME_COMPRESSION_RATIO_ARG, cmd.getOptionValue( TIME_COMPRESSION_RATIO_ARG ) );
        }

        if ( cmd.hasOption( VALIDATION_PARAMS_SIZE_ARG ) )
        {
            cmdParams.put( VALIDATION_PARAMS_SIZE_ARG, cmd.getOptionValue( VALIDATION_PARAMS_SIZE_ARG ) );
        }

        if ( cmd.hasOption( VALIDATION_SERIALIZATION_CHECK_ARG ) )
        {
            cmdParams.put( VALIDATION_SERIALIZATION_CHECK_ARG, cmd.getOptionValue( VALIDATION_SERIALIZATION_CHECK_ARG ) );
        }

        if ( cmd.hasOption( DB_VALIDATION_FILE_PATH_ARG ) )
        {
            cmdParams.put( DB_VALIDATION_FILE_PATH_ARG, cmd.getOptionValue( DB_VALIDATION_FILE_PATH_ARG ) );
        }


        if ( cmd.hasOption( SPINNER_SLEEP_DURATION_ARG ) )
        {
            cmdParams.put( SPINNER_SLEEP_DURATION_ARG, cmd.getOptionValue( SPINNER_SLEEP_DURATION_ARG ) );
        }

        if ( cmd.hasOption( HELP_ARG ) )
        {
            cmdParams.put( HELP_ARG, Boolean.toString( true ) );
        }

        if ( cmd.hasOption( IGNORE_SCHEDULED_START_TIMES_ARG ) )
        {
            cmdParams.put( IGNORE_SCHEDULED_START_TIMES_ARG, Boolean.toString( true ) );
        }

        if ( cmd.hasOption( WARMUP_COUNT_ARG ) )
        {
            cmdParams.put( WARMUP_COUNT_ARG, cmd.getOptionValue( WARMUP_COUNT_ARG ) );
        }

        if ( cmd.hasOption( SKIP_COUNT_ARG ) )
        {
            cmdParams.put( SKIP_COUNT_ARG, cmd.getOptionValue( SKIP_COUNT_ARG ) );
        }

        if ( cmd.hasOption( PROPERTY_FILE_ARG ) )
        {
            for ( String propertyFilePath : cmd.getOptionValues( PROPERTY_FILE_ARG ) )
            {
                // code assumes ordering -> first files more important than last, first values get priority
                try
                {
                    Properties tempFileProperties = new Properties();
                    tempFileProperties.load( new FileInputStream( propertyFilePath ) );
                    Map<String,String> tempFileParams = MapUtils.propertiesToMap( tempFileProperties );
                    boolean overwrite = true;
                    fileParams = MapUtils.mergeMaps(
                            convertLongKeysToShortKeys( tempFileParams ),
                            fileParams,
                            overwrite );
                }
                catch ( IOException e )
                {
                    throw new ParseException(
                            format( "Error loading properties file %s\n%s", propertyFilePath, e.getMessage() ) );
                }
            }
        }

        if ( cmd.hasOption( PROPERTY_ARG ) )
        {
            for ( Entry<Object,Object> cmdProperty : cmd.getOptionProperties( PROPERTY_ARG ).entrySet() )
            {
                cmdParams.put( (String) cmdProperty.getKey(), (String) cmdProperty.getValue() );
            }
        }

        boolean overwrite = true;
        return MapUtils.mergeMaps(
                convertLongKeysToShortKeys( fileParams ),
                convertLongKeysToShortKeys( cmdParams ),
                overwrite );
    }

    public static Map<String,String> convertLongKeysToShortKeys( Map<String,String> paramsMap )
    {
        paramsMap = replaceKey( paramsMap, MODE_ARG_LONG,  MODE_ARG);
        paramsMap = replaceKey( paramsMap, OPERATION_COUNT_ARG_LONG, OPERATION_COUNT_ARG );
        paramsMap = replaceKey( paramsMap, NAME_ARG_LONG, NAME_ARG );
        paramsMap = replaceKey( paramsMap, WORKLOAD_ARG_LONG, WORKLOAD_ARG );
        paramsMap = replaceKey( paramsMap, DB_ARG_LONG, DB_ARG );
        paramsMap = replaceKey( paramsMap, THREADS_ARG_LONG, THREADS_ARG );
        paramsMap = replaceKey( paramsMap, SHOW_STATUS_ARG_LONG, SHOW_STATUS_ARG );
        paramsMap = replaceKey( paramsMap, TIME_UNIT_ARG_LONG, TIME_UNIT_ARG );
        paramsMap = replaceKey( paramsMap, RESULT_DIR_PATH_ARG_LONG, RESULT_DIR_PATH_ARG );
        paramsMap = replaceKey( paramsMap, TIME_COMPRESSION_RATIO_ARG_LONG, TIME_COMPRESSION_RATIO_ARG );
        paramsMap = replaceKey( paramsMap, VALIDATION_PARAMS_SIZE_ARG_LONG, VALIDATION_PARAMS_SIZE_ARG );
        paramsMap = replaceKey( paramsMap, VALIDATION_SERIALIZATION_CHECK_ARG_LONG, VALIDATION_SERIALIZATION_CHECK_ARG );
        paramsMap = replaceKey( paramsMap, DB_VALIDATION_FILE_PATH_ARG_LONG, DB_VALIDATION_FILE_PATH_ARG );
        paramsMap = replaceKey( paramsMap, SPINNER_SLEEP_DURATION_ARG_LONG, SPINNER_SLEEP_DURATION_ARG );
        paramsMap = replaceKey( paramsMap, WARMUP_COUNT_ARG_LONG, WARMUP_COUNT_ARG );
        paramsMap = replaceKey( paramsMap, SKIP_COUNT_ARG_LONG, SKIP_COUNT_ARG );
        return paramsMap;
    }

    // NOTE: not safe in general case, no check for duplicate keys, i.e., if newKey already exists its value will be
    // overwritten
    private static Map<String,String> replaceKey( Map<String,String> paramsMap, String oldKey, String newKey )
    {
        if ( false == paramsMap.containsKey( oldKey ) )
        {
            return paramsMap;
        }
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
        Option modeOption =
            OptionBuilder.hasArgs( 1 ).withArgName( "mode" ).withDescription( MODE_DESCRIPTION )
                .withLongOpt(MODE_ARG_LONG).create( MODE_ARG );
        options.addOption( modeOption );

        Option dbOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "classname" ).withDescription( DB_DESCRIPTION ).withLongOpt(
                        DB_ARG_LONG ).create( DB_ARG );
        options.addOption( dbOption );

        Option workloadOption = OptionBuilder.hasArgs( 1 ).withArgName( "classname" ).withDescription(
                WORKLOAD_DESCRIPTION ).withLongOpt( WORKLOAD_ARG_LONG ).create( WORKLOAD_ARG );
        options.addOption( workloadOption );

        Option operationCountOption = OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription(
                OPERATION_COUNT_DESCRIPTION ).withLongOpt( OPERATION_COUNT_ARG_LONG ).create( OPERATION_COUNT_ARG );
        options.addOption( operationCountOption );

        /*
         * Optional
         */
        Option nameOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "name" ).withDescription( NAME_DESCRIPTION ).withLongOpt(
                        NAME_ARG_LONG ).create( NAME_ARG );
        options.addOption( nameOption );

        Option resultFileOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "path" ).withDescription( RESULT_DIR_PATH_DESCRIPTION )
                        .withLongOpt(
                                RESULT_DIR_PATH_ARG_LONG ).create( RESULT_DIR_PATH_ARG );
        options.addOption( resultFileOption );

        Option threadsOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription( THREADS_DESCRIPTION ).withLongOpt(
                        THREADS_ARG_LONG ).create( THREADS_ARG );
        options.addOption( threadsOption );

        Option statusOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "seconds" ).withDescription( SHOW_STATUS_DESCRIPTION )
                        .withLongOpt(
                                SHOW_STATUS_ARG_LONG ).create( SHOW_STATUS_ARG );
        options.addOption( statusOption );

        Option timeUnitOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "unit" ).withDescription( TIME_UNIT_DESCRIPTION ).withLongOpt(
                        TIME_UNIT_ARG_LONG ).create( TIME_UNIT_ARG );
        options.addOption( timeUnitOption );

        Option timeCompressionRatioOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "ratio" ).withDescription( TIME_COMPRESSION_RATIO_DESCRIPTION )
                        .withLongOpt(
                                TIME_COMPRESSION_RATIO_ARG_LONG ).create( TIME_COMPRESSION_RATIO_ARG );
        options.addOption( timeCompressionRatioOption );

        Option validationSerializationOption =
                OptionBuilder.withDescription( VALIDATION_SERIALIZATION_CHECK_DESCRIPTION ).withLongOpt(
                    VALIDATION_SERIALIZATION_CHECK_ARG_LONG ).create( VALIDATION_SERIALIZATION_CHECK_ARG );
        options.addOption( validationSerializationOption );

        Option validationParamsSizeOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription( VALIDATION_PARAMS_SIZE_DESCRIPTION )
                        .withLongOpt(
                                VALIDATION_PARAMS_SIZE_ARG_LONG ).create( VALIDATION_PARAMS_SIZE_ARG );
        options.addOption( validationParamsSizeOption );

        Option databaseValidationFilePathOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "path" ).withDescription( DB_VALIDATION_FILE_PATH_DESCRIPTION )
                        .withLongOpt(
                                DB_VALIDATION_FILE_PATH_ARG_LONG ).create( DB_VALIDATION_FILE_PATH_ARG );
        options.addOption( databaseValidationFilePathOption );

        Option spinnerSleepDurationOption = OptionBuilder.hasArgs( 1 ).withArgName( "duration" )
                .withDescription( SPINNER_SLEEP_DURATION_DESCRIPTION ).withLongOpt(
                        SPINNER_SLEEP_DURATION_ARG_LONG ).create( SPINNER_SLEEP_DURATION_ARG );
        options.addOption( spinnerSleepDurationOption );

        Option warmupCountOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription( WARMUP_COUNT_DESCRIPTION )
                        .withLongOpt( WARMUP_COUNT_ARG_LONG ).create( WARMUP_COUNT_ARG );
        options.addOption( warmupCountOption );

        Option skipCountOption =
                OptionBuilder.hasArgs( 1 ).withArgName( "count" ).withDescription( SKIP_COUNT_DESCRIPTION )
                        .withLongOpt( SKIP_COUNT_ARG_LONG ).create( SKIP_COUNT_ARG );
        options.addOption( skipCountOption );

        Option printHelpOption = OptionBuilder.withDescription( HELP_DESCRIPTION ).create( HELP_ARG );
        options.addOption( printHelpOption );


        Option flushOption = OptionBuilder.withDescription(FLUSH_LOG_DESCRIPTION).create(FLUSH_LOG_ARG);
        options.addOption( flushOption );

        Option ignoreScheduledStartTimesOption =
                OptionBuilder.withDescription( IGNORE_SCHEDULED_START_TIMES_DESCRIPTION )
                        .create( IGNORE_SCHEDULED_START_TIMES_ARG );
        options.addOption( ignoreScheduledStartTimesOption );

        Option propertyFileOption = OptionBuilder.hasArgs().withValueSeparator( COMMANDLINE_SEPARATOR_CHAR )
                .withArgName( "file1" + COMMANDLINE_SEPARATOR_CHAR + "file2" ).withDescription(
                        PROPERTY_FILE_DESCRIPTION ).create( PROPERTY_FILE_ARG );
        options.addOption( propertyFileOption );

        Option propertyOption = OptionBuilder.hasArgs( 2 ).withValueSeparator( COMMANDLINE_SEPARATOR_CHAR )
                .withArgName( "key" + COMMANDLINE_SEPARATOR_CHAR + "value" ).withDescription(
                        PROPERTY_DESCRIPTION ).create( PROPERTY_ARG );
        options.addOption( propertyOption );

        return options;
    }

    /**
     * Returns a HashSet of Strings
     * @return
     */
    private static Set<String> coreConfigurationParameterKeys()
    {
        return Sets.newHashSet(
                MODE_ARG,
                NAME_ARG,
                DB_ARG,
                WORKLOAD_ARG,
                OPERATION_COUNT_ARG,
                THREADS_ARG,
                SHOW_STATUS_ARG,
                TIME_UNIT_ARG,
                RESULT_DIR_PATH_ARG,
                TIME_COMPRESSION_RATIO_ARG,
                VALIDATION_PARAMS_SIZE_ARG,
                VALIDATION_SERIALIZATION_CHECK_ARG,
                DB_VALIDATION_FILE_PATH_ARG,
                SPINNER_SLEEP_DURATION_ARG,
                HELP_ARG,
                IGNORE_SCHEDULED_START_TIMES_ARG,
                WARMUP_COUNT_ARG,
                SKIP_COUNT_ARG
        );
    }

    /**
     * Retrieve the usage information of the driver as string
     * @return The usage information of the driver as string
     */
    public static String commandlineHelpString()
    {
        Options options = OPTIONS;
        int printedRowWidth = 110;
        String header = "";
        String footer = "";
        int spacesBeforeOption = 3;
        int spacesBeforeOptionDescription = 5;
        boolean displayUsage = true;
        String commandLineSyntax = "java -cp driver-VERSION.jar " + Client.class.getName();
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter( os );
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp( writer, printedRowWidth, commandLineSyntax, header, options, spacesBeforeOption,
                spacesBeforeOptionDescription, footer, displayUsage );
        writer.flush();
        writer.close();
        return os.toString();
    }

    private final Map<String,String> paramsMap;
    private final String mode;
    private final String name;
    private final String dbClassName;
    private final String workloadClassName;
    private final long operationCount;
    private final int threadCount;
    private final int statusDisplayIntervalAsSeconds;
    private final TimeUnit timeUnit;
    private final String resultDirPath;
    private final double timeCompressionRatio;
    private final int validationParametersSize;
    private final boolean validationSerializationCheck;
    private final String databaseValidationFilePath;
    private final long spinnerSleepDurationAsMilli;
    private final boolean printHelp;
    private final boolean ignoreScheduledStartTimes;
    private final long warmupCount;
    private final long skipCount;
    private final boolean flushLog;

    public ConsoleAndFileDriverConfiguration( Map<String,String> paramsMap,
            String mode,
            String name,
            String dbClassName,
            String workloadClassName,
            long operationCount,
            int threadCount,
            int statusDisplayIntervalAsSeconds,
            TimeUnit timeUnit,
            String resultDirPath,
            double timeCompressionRatio,
            int validationParametersSize,
            boolean validationSerializationCheck,
            String databaseValidationFilePath,
            long spinnerSleepDurationAsMilli,
            boolean printHelp,
            boolean ignoreScheduledStartTimes,
            long warmupCount,
            long skipCount,
            boolean flushLog )
    {
        if ( null == paramsMap )
        {
            paramsMap = new HashMap<>();
        }
        this.paramsMap = paramsMap;
        this.mode = mode;
        this.name = name;
        this.dbClassName = dbClassName;
        this.workloadClassName = workloadClassName;
        this.operationCount = operationCount;
        this.threadCount = threadCount;
        this.statusDisplayIntervalAsSeconds = statusDisplayIntervalAsSeconds;
        this.timeUnit = timeUnit;
        this.resultDirPath = resultDirPath;
        this.timeCompressionRatio = timeCompressionRatio;
        this.validationParametersSize = validationParametersSize;
        this.validationSerializationCheck = validationSerializationCheck;
        this.databaseValidationFilePath = databaseValidationFilePath;
        this.spinnerSleepDurationAsMilli = spinnerSleepDurationAsMilli;
        this.printHelp = printHelp;
        this.ignoreScheduledStartTimes = ignoreScheduledStartTimes;
        this.warmupCount = warmupCount;
        this.skipCount = skipCount;
        this.flushLog = flushLog;

        if ( null != mode )
        {
            paramsMap.put( MODE_ARG, mode );
        }

        if ( null != name )
        {
            paramsMap.put( NAME_ARG, name );
        }
        if ( null != dbClassName )
        {
            paramsMap.put( DB_ARG, dbClassName );
        }
        paramsMap.put( OPERATION_COUNT_ARG, Long.toString( operationCount ) );
        if ( null != workloadClassName )
        {
            paramsMap.put( WORKLOAD_ARG, workloadClassName );
        }
        paramsMap.put( THREADS_ARG, Integer.toString( threadCount ) );
        paramsMap.put( SHOW_STATUS_ARG, Integer.toString( statusDisplayIntervalAsSeconds ) );
        paramsMap.put( TIME_UNIT_ARG, timeUnit.name() );
        if ( null != resultDirPath )
        {
            paramsMap.put( RESULT_DIR_PATH_ARG, resultDirPath );
        }
        paramsMap.put( TIME_COMPRESSION_RATIO_ARG, Double.toString( timeCompressionRatio ) );

        paramsMap.put( SPINNER_SLEEP_DURATION_ARG, Long.toString( spinnerSleepDurationAsMilli ) );
        paramsMap.put( HELP_ARG, Boolean.toString( printHelp ) );
        paramsMap.put( IGNORE_SCHEDULED_START_TIMES_ARG, Boolean.toString( ignoreScheduledStartTimes ) );
        paramsMap.put( WARMUP_COUNT_ARG, Long.toString( warmupCount ) );
        paramsMap.put( SKIP_COUNT_ARG, Long.toString( skipCount ) );
        paramsMap.put( FLUSH_LOG_ARG, Boolean.toString( flushLog ) );
        // Validation specific
        if ( null != databaseValidationFilePath )
        {
            paramsMap.put( DB_VALIDATION_FILE_PATH_ARG, databaseValidationFilePath );
        }
        paramsMap.put( VALIDATION_PARAMS_SIZE_ARG, Integer.toString(validationParametersSize) );
        paramsMap.put( VALIDATION_SERIALIZATION_CHECK_ARG, Boolean.toString(validationSerializationCheck) );
    }

    @Override
    public String mode()
    {
        return mode;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public String dbClassName()
    {
        return dbClassName;
    }

    @Override
    public String workloadClassName()
    {
        return workloadClassName;
    }

    @Override
    public long operationCount()
    {
        return operationCount;
    }

    @Override
    public int threadCount()
    {
        return threadCount;
    }

    @Override
    public int statusDisplayIntervalAsSeconds()
    {
        return statusDisplayIntervalAsSeconds;
    }

    @Override
    public TimeUnit timeUnit()
    {
        return timeUnit;
    }

    @Override
    public String resultDirPath()
    {
        if ( null == resultDirPath )
        {
            return null;
        }
        else
        {
            return new File( resultDirPath ).getAbsolutePath();
        }
    }

    @Override
    public double timeCompressionRatio()
    {
        return timeCompressionRatio;
    }

    @Override
    public boolean validationSerializationCheck()
    {
        return validationSerializationCheck;
    }

    @Override
    public int validationParametersSize()
    {
        return validationParametersSize;
    }

    @Override
    public String databaseValidationFilePath()
    {
        return databaseValidationFilePath;
    }

    @Override
    public long spinnerSleepDurationAsMilli()
    {
        return spinnerSleepDurationAsMilli;
    }

    @Override
    public boolean shouldPrintHelpString()
    {
        return printHelp;
    }

    @Override
    public String helpString()
    {
        return ConsoleAndFileDriverConfiguration.commandlineHelpString();
    }

    @Override
    public boolean ignoreScheduledStartTimes()
    {
        return ignoreScheduledStartTimes;
    }

    @Override
    public long warmupCount()
    {
        return warmupCount;
    }

    @Override
    public long skipCount()
    {
        return skipCount;
    }

    @Override
    public boolean flushLog() { return flushLog; }

    @Override
    public Map<String,String> asMap()
    {
        return paramsMap;
    }

    /**
     * Returns a new DriverConfiguration instance.
     * New instance has the same values for all fields except those that have an associated value in the new config.
     * New instance contains all fields that original configuration instance contained.
     * New instance will contain additional fields if new config introduced any.
     *
     * @param newConfiguration
     * @return
     * @throws DriverConfigurationException
     */
    @Override
    public DriverConfiguration applyArgs( DriverConfiguration newConfiguration ) throws DriverConfigurationException
    {
        return applyArgs( newConfiguration.asMap() );
    }

    /**
     * Returns a new DriverConfiguration instance.
     * New instance has the same values for all fields except the one that was set/changed.
     * New instance contains all fields that original configuration instance contained.
     * New instance will contain additional fields if the new parameter introduced one.
     *
     * @param argument
     * @param newValue
     * @return
     * @throws DriverConfigurationException
     */
    public DriverConfiguration applyArg( String argument, String newValue ) throws DriverConfigurationException
    {
        Map<String,String> newParamsMap = new HashMap<>();
        newParamsMap.put( argument, newValue );
        return applyArgs( newParamsMap );
    }

    /**
     * Returns a new DriverConfiguration instance.
     * New instance has the same values for all fields except those that have an associated value in the new parameters
     * map.
     * New instance contains all fields that original configuration instance contained.
     * New instance will contain additional fields if new parameters map introduced any.
     *
     * @param newParamsMap
     * @return
     * @throws DriverConfigurationException
     */
    @Override
    public DriverConfiguration applyArgs( Map<String,String> newParamsMap ) throws DriverConfigurationException
    {
        Map<String,String> newParamsMapWithShortKeys = convertLongKeysToShortKeys( newParamsMap );
        Map<String,String> newOtherParams = MapUtils.mergeMaps( this.paramsMap, newParamsMapWithShortKeys, true );

        String newMode = (newParamsMapWithShortKeys.containsKey( MODE_ARG )) ?
                         newParamsMapWithShortKeys.get( MODE_ARG ) :
                         mode;

        String newName = (newParamsMapWithShortKeys.containsKey( NAME_ARG )) ?
                         newParamsMapWithShortKeys.get( NAME_ARG ) :
                         name;
        String newDbClassName = (newParamsMapWithShortKeys.containsKey( DB_ARG )) ?
                                newParamsMapWithShortKeys.get( DB_ARG ) :
                                dbClassName;
        String newWorkloadClassName = (newParamsMapWithShortKeys.containsKey( WORKLOAD_ARG )) ?
                                      newParamsMapWithShortKeys.get( WORKLOAD_ARG ) :
                                      workloadClassName;
        long newOperationCount = (newParamsMapWithShortKeys.containsKey( OPERATION_COUNT_ARG )) ?
                                 Long.parseLong( newParamsMapWithShortKeys.get( OPERATION_COUNT_ARG ) ) :
                                 operationCount;
        int newThreadCount = (newParamsMapWithShortKeys.containsKey( THREADS_ARG )) ?
                             Integer.parseInt( newParamsMapWithShortKeys.get( THREADS_ARG ) ) :
                             threadCount;
        int newStatusDisplayIntervalAsSeconds = (newParamsMapWithShortKeys.containsKey( SHOW_STATUS_ARG )) ?
                                                Integer.parseInt( newParamsMapWithShortKeys.get( SHOW_STATUS_ARG ) ) :
                                                statusDisplayIntervalAsSeconds;
        TimeUnit newTimeUnit = (newParamsMapWithShortKeys.containsKey( TIME_UNIT_ARG )) ?
                               TimeUnit.valueOf( newParamsMapWithShortKeys.get( TIME_UNIT_ARG ) ) :
                               timeUnit;
        String newResultDirPath = (newParamsMapWithShortKeys.containsKey( RESULT_DIR_PATH_ARG )) ?
                                  newParamsMapWithShortKeys.get( RESULT_DIR_PATH_ARG ) :
                                  resultDirPath;
        double newTimeCompressionRatio = (newParamsMapWithShortKeys.containsKey( TIME_COMPRESSION_RATIO_ARG )) ?
                                         Double.parseDouble(
                                                 newParamsMapWithShortKeys.get( TIME_COMPRESSION_RATIO_ARG ) ) :
                                         timeCompressionRatio;

        int newValidationParametersSize = (newParamsMapWithShortKeys.containsKey( VALIDATION_PARAMS_SIZE_ARG )) ?
                                               Integer.parseInt(newParamsMapWithShortKeys.get( VALIDATION_PARAMS_SIZE_ARG )) :
                                               validationParametersSize;

        boolean newValidationSerializationCheck =
            (newParamsMapWithShortKeys.containsKey( VALIDATION_SERIALIZATION_CHECK_ARG )) ?
            Boolean.parseBoolean( newParamsMapWithShortKeys.get( VALIDATION_SERIALIZATION_CHECK_ARG ) ) :
            validationSerializationCheck;

        String newDatabaseValidationFilePath = (newParamsMapWithShortKeys.containsKey( DB_VALIDATION_FILE_PATH_ARG )) ?
                                               newParamsMapWithShortKeys.get( DB_VALIDATION_FILE_PATH_ARG ) :
                                               databaseValidationFilePath;
        long newSpinnerSleepDurationAsMilli = (newParamsMapWithShortKeys.containsKey( SPINNER_SLEEP_DURATION_ARG )) ?
                                              Long.parseLong(
                                                      (newParamsMapWithShortKeys.get( SPINNER_SLEEP_DURATION_ARG )) ) :
                                              spinnerSleepDurationAsMilli;
        boolean newPrintHelp = (newParamsMapWithShortKeys.containsKey( HELP_ARG )) ?
                               Boolean.parseBoolean( newParamsMapWithShortKeys.get( HELP_ARG ) ) :
                               printHelp;
        boolean newIgnoreScheduledStartTimes =
                (newParamsMapWithShortKeys.containsKey( IGNORE_SCHEDULED_START_TIMES_ARG )) ?
                Boolean.parseBoolean( newParamsMapWithShortKeys.get( IGNORE_SCHEDULED_START_TIMES_ARG ) ) :
                ignoreScheduledStartTimes;
        long newWarmupCount = (newParamsMapWithShortKeys.containsKey( WARMUP_COUNT_ARG )) ?
                              Long.parseLong( newParamsMapWithShortKeys.get( WARMUP_COUNT_ARG ) ) :
                              warmupCount;
        long newSkipCount = (newParamsMapWithShortKeys.containsKey( SKIP_COUNT_ARG )) ?
                            Long.parseLong( newParamsMapWithShortKeys.get( SKIP_COUNT_ARG ) ) :
                            skipCount;
        boolean newFlushLog = (newParamsMapWithShortKeys.containsKey( FLUSH_LOG_ARG )) ?
                        Boolean.parseBoolean( newParamsMapWithShortKeys.get( FLUSH_LOG_ARG ) ) :
                        flushLog;

        return new ConsoleAndFileDriverConfiguration(
                newOtherParams,
                newMode,
                newName,
                newDbClassName,
                newWorkloadClassName,
                newOperationCount,
                newThreadCount,
                newStatusDisplayIntervalAsSeconds,
                newTimeUnit,
                newResultDirPath,
                newTimeCompressionRatio,
                newValidationParametersSize,
                newValidationSerializationCheck,
                newDatabaseValidationFilePath,
                newSpinnerSleepDurationAsMilli,
                newPrintHelp,
                newIgnoreScheduledStartTimes,
                newWarmupCount,
                newSkipCount,
                newFlushLog
        );
    }

    public String[] toArgs() throws DriverConfigurationException
    {
        List<String> argsList = new ArrayList<>();
        // required core parameters
        if ( null != mode )
        {
            argsList.addAll( Lists.newArrayList( "-" + MODE_ARG, mode ) );
        }

        if ( null != dbClassName )
        {
            argsList.addAll( Lists.newArrayList( "-" + DB_ARG, dbClassName ) );
        }
        if ( null != workloadClassName )
        {
            argsList.addAll( Lists.newArrayList( "-" + WORKLOAD_ARG, workloadClassName ) );
        }
        argsList.addAll( Lists.newArrayList( "-" + OPERATION_COUNT_ARG, Long.toString( operationCount ) ) );
        // optional core parameters
        argsList.addAll( Lists.newArrayList( "-" + SHOW_STATUS_ARG, Long.toString( statusDisplayIntervalAsSeconds ) ) );
        argsList.addAll( Lists.newArrayList( "-" + THREADS_ARG, Integer.toString( threadCount ) ) );
        argsList.addAll( Lists.newArrayList( "-" + WARMUP_COUNT_ARG, Long.toString( warmupCount ) ) );
        argsList.addAll( Lists.newArrayList( "-" + SKIP_COUNT_ARG, Long.toString( skipCount ) ) );
        if ( null != name )
        {
            argsList.addAll( Lists.newArrayList( "-" + NAME_ARG, name ) );
        }
        if ( null != resultDirPath )
        {
            argsList.addAll( Lists.newArrayList( "-" + RESULT_DIR_PATH_ARG, resultDirPath ) );
        }
        argsList.addAll( Lists.newArrayList( "-" + TIME_UNIT_ARG, timeUnit.name() ) );
        argsList.addAll( Lists.newArrayList( "-" + TIME_COMPRESSION_RATIO_ARG, Double.toString( timeCompressionRatio ) ) );

        if ( validationSerializationCheck == false){
            argsList.addAll( Lists.newArrayList( "-" + VALIDATION_SERIALIZATION_CHECK_ARG, Boolean.toString(validationSerializationCheck ) ));
        }
        if ( null != databaseValidationFilePath )
        {
            argsList.addAll( Lists.newArrayList( "-" + DB_VALIDATION_FILE_PATH_ARG, databaseValidationFilePath ) );
        }

        argsList.addAll(
                Lists.newArrayList( "-" + SPINNER_SLEEP_DURATION_ARG, Long.toString( spinnerSleepDurationAsMilli ) ) );
        if ( printHelp )
        {
            argsList.add( "-" + HELP_ARG );
        }
        if ( ignoreScheduledStartTimes )
        {
            argsList.add( "-" + IGNORE_SCHEDULED_START_TIMES_ARG );
        }
        // additional, workload/database-related params
        Map<String,String> additionalParameters =
                MapUtils.copyExcludingKeys( paramsMap, coreConfigurationParameterKeys() );
        for ( Entry<String,String> additionalParam : MapUtils.sortedEntrySet( additionalParameters ) )
        {
            argsList.addAll( Lists.newArrayList( "-p", additionalParam.getKey(), additionalParam.getValue() ) );
        }
        return argsList.toArray( new String[argsList.size()] );
    }

    @Override
    public String toPropertiesString() throws DriverConfigurationException
    {
        StringBuilder sb = new StringBuilder();
        sb.append( "# -------------------------------------\n" );
        sb.append( "# -------------------------------------\n" );
        sb.append( "# ----- LDBC Driver Configuration -----\n" );
        sb.append( "# -------------------------------------\n" );
        sb.append( "# -------------------------------------\n" );
        sb.append( "\n" );
        sb.append( "# ***********************\n" );
        sb.append( "# *** driver defaults ***\n" );
        sb.append( "# ***********************\n" );
        sb.append( "\n" );
        sb.append( "# status display interval (intermittently show status during benchmark execution)\n" );
        sb.append( "# STRING\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( MODE_ARG ).append( "/--" ).append( MODE_ARG_LONG )
                .append( "\n" );
        sb.append( "# INT-32 (seconds)\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( SHOW_STATUS_ARG ).append( "/--" )
                .append( SHOW_STATUS_ARG_LONG ).append( "\n" );
        sb.append( SHOW_STATUS_ARG_LONG ).append( "=" ).append( statusDisplayIntervalAsSeconds ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# thread pool size to use for executing operation handlers\n" );
        sb.append( "# INT-32\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( THREADS_ARG ).append( "/--" ).append( THREADS_ARG_LONG )
                .append( "\n" );
        sb.append( THREADS_ARG_LONG ).append( "=" ).append( threadCount ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# number of operations to execute during warmup phase of workload\n" );
        sb.append( "# INT-64\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( WARMUP_COUNT_ARG ).append( "/--" )
                .append( WARMUP_COUNT_ARG_LONG ).append( "\n" );
        sb.append( WARMUP_COUNT_ARG_LONG ).append( "=" ).append( warmupCount ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# number of operations to skip before beginning workload execution\n" );
        sb.append( "# INT-64\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( SKIP_COUNT_ARG ).append( "/--" )
                .append( SKIP_COUNT_ARG_LONG ).append( "\n" );
        sb.append( SKIP_COUNT_ARG_LONG ).append( "=" ).append( skipCount ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# name of the benchmark run\n" );
        sb.append( "# STRING\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( NAME_ARG ).append( "/--" ).append( NAME_ARG_LONG )
                .append( "\n" );
        if ( null == name )
        {
            sb.append( "# " ).append( NAME_ARG_LONG ).append( "=" ).append( "\n" );
        }
        else
        {
            sb.append( NAME_ARG_LONG ).append( "=" ).append( name ).append( "\n" );
        }
        sb.append( "\n" );
        sb.append( "# path specifying where to write the benchmark results file\n" );
        sb.append( "# STRING\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( RESULT_DIR_PATH_ARG ).append( "/--" )
                .append( RESULT_DIR_PATH_ARG_LONG ).append( "\n" );
        if ( null == resultDirPath )
        {
            sb.append( "# " ).append( RESULT_DIR_PATH_ARG_LONG ).append( "=" ).append( "\n" );
        }
        else
        {
            sb.append( RESULT_DIR_PATH_ARG_LONG ).append( "=" ).append( resultDirPath ).append( "\n" );
        }
        sb.append( "\n" );
        sb.append( "# time unit to use for measuring performance metrics (e.g., query response time)\n" );
        sb.append( "# ENUM (" ).append( Arrays.toString( VALID_TIME_UNITS ) ).append( ")\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( TIME_UNIT_ARG ).append( "/--" ).append( TIME_UNIT_ARG_LONG )
                .append( "\n" );
        sb.append( TIME_UNIT_ARG_LONG ).append( "=" ).append( timeUnit ).append( "\n" );
        sb.append( "\n" );
        sb.append(
                "# used to 'compress'/'stretch' durations between operation start times to increase/decrease " +
                "benchmark load\n" );
        sb.append( "# e.g. 2.0 = run benchmark 2x slower, 0.1 = run benchmark 10x faster\n" );
        sb.append( "# DOUBLE\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( TIME_COMPRESSION_RATIO_ARG ).append( "/--" )
                .append( TIME_COMPRESSION_RATIO_ARG_LONG ).append( "\n" );
        sb.append( TIME_COMPRESSION_RATIO_ARG_LONG ).append( "=" ).append( timeCompressionRatio ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# enable validation that will check if the provided database implementation is correct\n" );
        sb.append( "# parameter value specifies where to find the validation parameters file\n" );
        sb.append( "# STRING\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( DB_VALIDATION_FILE_PATH_ARG ).append( "/--" )
                .append( DB_VALIDATION_FILE_PATH_ARG_LONG ).append( "\n" );
        if ( null == databaseValidationFilePath )
        {
            sb.append( "# " ).append( DB_VALIDATION_FILE_PATH_ARG_LONG ).append( "=" ).append( "\n" );
        }
        else
        {
            sb.append( DB_VALIDATION_FILE_PATH_ARG_LONG ).append( "=" ).append( databaseValidationFilePath )
                    .append( "\n" );
        }
        sb.append( "\n" );
        sb.append( "# generate validation parameters file for validating correctness of database implementations\n" );
        sb.append(
                "# parameter values specify: (1) where to create the validation parameters file (2) how many " +
                "validation parameters to generate\n" );
        sb.append( "# BOOLEAN\n");
        sb.append( "# COMMAND: " ).append( "-" ).append( VALIDATION_SERIALIZATION_CHECK_ARG ).append( "/--" )
                .append( VALIDATION_SERIALIZATION_CHECK_ARG_LONG ).append( "\n" );
        sb.append( "# INT-32\n");
        sb.append( "# COMMAND: " ).append( "-" ).append( VALIDATION_PARAMS_SIZE_ARG ).append( "/--" )
                        .append( VALIDATION_PARAMS_SIZE_ARG_LONG ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# sleep duration (ms) injected into busy wait loops (to reduce CPU consumption)\n" );
        sb.append( "# INT-64 (milliseconds)\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( SPINNER_SLEEP_DURATION_ARG ).append( "/--" )
                .append( SPINNER_SLEEP_DURATION_ARG_LONG ).append( "\n" );
        sb.append( SPINNER_SLEEP_DURATION_ARG_LONG ).append( "=" ).append( spinnerSleepDurationAsMilli ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# print help string - usage instructions\n" );
        sb.append( "# BOOLEAN\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( HELP_ARG ).append( "\n" );
        sb.append( HELP_ARG ).append( "=" ).append( printHelp ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# executes operations as fast as possible, ignoring their scheduled start times\n" );
        sb.append( "# BOOLEAN\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( IGNORE_SCHEDULED_START_TIMES_ARG ).append( "\n" );
        sb.append( IGNORE_SCHEDULED_START_TIMES_ARG ).append( "=" ).append( ignoreScheduledStartTimes ).append( "\n" );
        sb.append( "\n" );
        sb.append( "# ***************************************************************\n" );
        sb.append( "# *** the following should be set by workload implementations ***\n" );
        sb.append( "# ***************************************************************\n" );
        sb.append( "\n" );
        sb.append( "# fully qualified class name of the Workload (class) implementation to execute\n" );
        sb.append( "# STRING (e.g., " ).append( LdbcSnbInteractiveWorkload.class.getName() ).append( ")\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( WORKLOAD_ARG ).append( "/--" ).append( WORKLOAD_ARG_LONG )
                .append( "\n" );
        if ( null == workloadClassName )
        {
            sb.append( "# " ).append( WORKLOAD_ARG_LONG ).append( "=" ).append( "\n" );
        }
        else
        {
            sb.append( WORKLOAD_ARG_LONG ).append( "=" ).append( workloadClassName ).append( "\n" );
        }
        sb.append( "\n" );
        sb.append( "# number of operations to generate during benchmark execution\n" );
        sb.append( "# INT-64\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( OPERATION_COUNT_ARG ).append( "/--" )
                .append( OPERATION_COUNT_ARG_LONG ).append( "\n" );
        if ( 0 == operationCount )
        {
            sb.append( "# " ).append( OPERATION_COUNT_ARG_LONG ).append( "=" ).append( "\n" );
        }
        else
        {
            sb.append( OPERATION_COUNT_ARG_LONG ).append( "=" ).append( operationCount ).append( "\n" );
        }
        sb.append( "\n" );
        sb.append( "# ************************************************************************************\n" );
        sb.append( "# *** the following should be set by vendor implementations for specific workloads ***\n" );
        sb.append( "# ************************************************************************************\n" );
        sb.append( "\n" );
        sb.append( "# fully qualified class name of the Db (class) implementation to execute\n" );
        sb.append( "# STRING (e.g., " ).append( DummyLdbcSnbInteractiveDb.class.getName() ).append( ")\n" );
        sb.append( "# COMMAND: " ).append( "-" ).append( DB_ARG ).append( "/--" ).append( DB_ARG_LONG ).append( "\n" );
        if ( null == dbClassName )
        {
            sb.append( "# " ).append( DB_ARG_LONG ).append( "=" ).append( "\n" );
        }
        else
        {
            sb.append( DB_ARG_LONG ).append( "=" ).append( dbClassName ).append( "\n" );
        }
        // Write additional, workload/database-related keys as well
        Map<String,String> additionalConfigurationParameters =
                MapUtils.copyExcludingKeys( paramsMap, coreConfigurationParameterKeys() );
        if ( !additionalConfigurationParameters.isEmpty() )
        {
            sb.append( "\n" );
            sb.append( "# ************************************************************************************\n" );
            sb.append( "# *** non-core configuration parameters ***\n" );
            sb.append( "# ************************************************************************************\n" );
            sb.append( "\n" );
            for ( Entry<String,String> configurationParameter : MapUtils
                    .sortedEntrySet( additionalConfigurationParameters ) )
            {
                sb.append( configurationParameter.getKey() ).append( "=" ).append( configurationParameter.getValue() )
                        .append( "\n" );
            }
        }
        return sb.toString();
    }

    @Override
    public String toString()
    {
        int padRightDistance = 32;
        StringBuilder sb = new StringBuilder();
        sb.append( "Parameters:" ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Mode:" ) ).append( mode )
        .append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Name:" ) ).append( name )
                .append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "DB:" ) ).append( dbClassName )
                .append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Workload:" ) )
                .append( workloadClassName ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Operation Count:" ) )
                .append( INTEGRAL_FORMAT.format( operationCount ) ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Warmup Count:" ) )
                .append( INTEGRAL_FORMAT.format( warmupCount ) ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Skip Count:" ) )
                .append( INTEGRAL_FORMAT.format( skipCount ) ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Worker Threads:" ) )
                .append( threadCount ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Status Display Interval:" ) ).append(
                TEMPORAL_UTIL.milliDurationToString( TimeUnit.SECONDS.toMillis( statusDisplayIntervalAsSeconds ) ) )
                .append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Time Unit:" ) ).append( timeUnit )
                .append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Results Directory:" ) )
                .append( resultDirPath() ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Time Compression Ratio:" ) )
                .append( FLOAT_FORMAT.format( timeCompressionRatio ) ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Validation Parameter Size:" ) )
                .append( validationParametersSize ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Validation Serialization Check:" ) )
                .append( validationSerializationCheck ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Validation Database File:" ) )
                .append( databaseValidationFilePath ).append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Spinner Sleep Duration:" ) )
                .append( TEMPORAL_UTIL.milliDurationToString( spinnerSleepDurationAsMilli ) ).append( " / " )
                .append( spinnerSleepDurationAsMilli ).append( " (ms)\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Print Help:" ) ).append( printHelp )
                .append( "\n" );
        sb.append( "\t" ).append( format( "%1$-" + padRightDistance + "s", "Ignore Scheduled Start Times:" ) )
                .append( ignoreScheduledStartTimes ).append( "\n" );

        Set<String> excludedKeys = coreConfigurationParameterKeys();

        Map<String,String> filteredParamsMap =
                MapUtils.copyExcludingKeys( convertLongKeysToShortKeys( paramsMap ), excludedKeys );
        if ( !filteredParamsMap.isEmpty() )
        {
            sb.append( "\t" ).append( "User-defined parameters:" ).append( "\n" );
            sb.append( MapUtils.prettyPrint( filteredParamsMap, "\t\t" ) );
        }

        return sb.toString();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ConsoleAndFileDriverConfiguration that = (ConsoleAndFileDriverConfiguration) o;

        if ( mode != that.mode )
        {
            return false;
        }
        if ( operationCount != that.operationCount )
        {
            return false;
        }
        if ( warmupCount != that.warmupCount )
        {
            return false;
        }
        if ( skipCount != that.skipCount )
        {
            return false;
        }
        if ( printHelp != that.printHelp )
        {
            return false;
        }
        if ( ignoreScheduledStartTimes != that.ignoreScheduledStartTimes )
        {
            return false;
        }
        if ( threadCount != that.threadCount )
        {
            return false;
        }
        if ( Double.compare( that.timeCompressionRatio, timeCompressionRatio ) != 0 )
        {
            return false;
        }

        if ( validationSerializationCheck != that.validationSerializationCheck )
        {
            return false;
        }
        if ( validationParametersSize != that.validationParametersSize )
        {
            return false;
        }
        if ( databaseValidationFilePath != null ? !databaseValidationFilePath.equals( that.databaseValidationFilePath )
                                                : that.databaseValidationFilePath != null )
        {
            return false;
        }
        if ( dbClassName != null ? !dbClassName.equals( that.dbClassName ) : that.dbClassName != null )
        {
            return false;
        }
        if ( name != null ? !name.equals( that.name ) : that.name != null )
        {
            return false;
        }
        if ( resultDirPath != null ? !resultDirPath.equals( that.resultDirPath ) : that.resultDirPath != null )
        {
            return false;
        }
        if ( spinnerSleepDurationAsMilli != that.spinnerSleepDurationAsMilli )
        {
            return false;
        }
        if ( statusDisplayIntervalAsSeconds != that.statusDisplayIntervalAsSeconds )
        {
            return false;
        }
        if ( timeUnit != that.timeUnit )
        {
            return false;
        }
        if ( workloadClassName != null ? !workloadClassName.equals( that.workloadClassName )
                                       : that.workloadClassName != null )
        {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = name != null ? name.hashCode() : 0;
        result = 31 * result + (mode != null ? mode.hashCode() : 0);
        result = 31 * result + (dbClassName != null ? dbClassName.hashCode() : 0);
        result = 31 * result + (workloadClassName != null ? workloadClassName.hashCode() : 0);
        result = 31 * result + (int) (operationCount ^ (operationCount >>> 32));
        result = 31 * result + (int) (warmupCount ^ (warmupCount >>> 32));
        result = 31 * result + (int) (skipCount ^ (skipCount >>> 32));
        result = 31 * result + threadCount;
        result = 31 * result + statusDisplayIntervalAsSeconds;
        result = 31 * result + (timeUnit != null ? timeUnit.hashCode() : 0);
        result = 31 * result + (resultDirPath != null ? resultDirPath.hashCode() : 0);
        temp = Double.doubleToLongBits( timeCompressionRatio );
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        result = 31 * result + (int) (validationParametersSize ^ (validationParametersSize >>> 32));
        result = 31 * result + (validationSerializationCheck ? 1 : 0);
        result = 31 * result + (databaseValidationFilePath != null ? databaseValidationFilePath.hashCode() : 0);
        result = 31 * result + (int) (spinnerSleepDurationAsMilli ^ (spinnerSleepDurationAsMilli >>> 32));
        result = 31 * result + (printHelp ? 1 : 0);
        result = 31 * result + (ignoreScheduledStartTimes ? 1 : 0);
        return result;
    }
}
