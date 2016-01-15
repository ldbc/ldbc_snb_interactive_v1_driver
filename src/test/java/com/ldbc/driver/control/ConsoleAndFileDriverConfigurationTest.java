package com.ldbc.driver.control;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

public class ConsoleAndFileDriverConfigurationTest
{
    @Test
    public void applyShouldWork() throws DriverConfigurationException
    {
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( "db1", "workload1", 1 );

        assertThat( configuration.dbClassName(), equalTo( "db1" ) );
        assertThat( configuration.workloadClassName(), equalTo( "workload1" ) );
        assertThat( configuration.operationCount(), equalTo( 1l ) );

        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.DB_ARG, "db2" );
        assertThat( configuration.dbClassName(), equalTo( "db2" ) );

        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload2" );
        assertThat( configuration.workloadClassName(), equalTo( "workload2" ) );

        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "2" );
        assertThat( configuration.operationCount(), equalTo( 2l ) );
    }

    @Test
    public void applyMapShouldWork() throws DriverConfigurationException
    {
        ConsoleAndFileDriverConfiguration configuration1 =
                ConsoleAndFileDriverConfiguration.fromDefaults( "db1", "workload1", 1 );

        assertThat( configuration1.dbClassName(), equalTo( "db1" ) );
        assertThat( configuration1.workloadClassName(), equalTo( "workload1" ) );
        assertThat( configuration1.operationCount(), equalTo( 1l ) );

        Map<String,String> configurationUpdate2 = new HashMap<>();
        configurationUpdate2.put( ConsoleAndFileDriverConfiguration.DB_ARG, "db2" );
        configurationUpdate2.put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload2" );
        configurationUpdate2.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "2" );

        ConsoleAndFileDriverConfiguration configuration2 =
                (ConsoleAndFileDriverConfiguration) configuration1.applyArgs( configurationUpdate2 );

        assertThat( configuration2.dbClassName(), equalTo( "db2" ) );
        assertThat( configuration2.workloadClassName(), equalTo( "workload2" ) );
        assertThat( configuration2.operationCount(), equalTo( 2l ) );
    }

    @Test
    public void toMapThenFromMapShouldEqualEvenWhenRequiredParamsAreNotSet() throws DriverConfigurationException
    {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 2;

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );

        DriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromParamsMap( configurationBefore.asMap() );

        assertThat( configurationBefore, equalTo( configurationAfter ) );
    }

    @Test
    public void toMapThenFromMapShouldReturnSameResultWhenAllParamsAreInitiallySetViaConstructor()
            throws DriverConfigurationException
    {
        long operationCount = 2;
        int threadCount = 4;
        int statusDisplayInterval = 1000;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = "results dir";
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = new HashSet<>();
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationCreationParams = null;
        String databaseValidationFilePath = null;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        String name = "LDBC-SNB";
        boolean ignoreScheduledStartTimes = true;
        long warmupCount = 5;
        long skipCount = 6;
        Map<String,String> paramsMap = new HashMap<>();

        ConsoleAndFileDriverConfiguration configurationBefore = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                name,
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationCreationParams,
                databaseValidationFilePath,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount
        );

        DriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromParamsMap( configurationBefore.asMap() );

        assertThat( configurationBefore, equalTo( configurationAfter ) );
    }

    @Test
    public void toArgsThenFromArgsShouldEqualEvenWhenRequiredParamsAreNotSet() throws DriverConfigurationException
    {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 2;

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );

        DriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromArgs( configurationBefore.toArgs() );

        assertThat( configurationBefore, equalTo( configurationAfter ) );
    }

    @Test
    public void toArgsThenFromArgsShouldReturnSameResultWhenAllParamsThatCanBeEmptyAreEmpty()
            throws DriverConfigurationException
    {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 2;

        Map<String,String> paramsFromPublicStaticDefaultValuesAsMap = new HashMap<>();
        // required params
        paramsFromPublicStaticDefaultValuesAsMap.put( ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName );
        paramsFromPublicStaticDefaultValuesAsMap
                .put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName );
        paramsFromPublicStaticDefaultValuesAsMap
                .put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( operationCount ) );

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromParamsMap( paramsFromPublicStaticDefaultValuesAsMap );

        ConsoleAndFileDriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromArgs( configurationBefore.toArgs() );

        assertThat( configurationBefore, equalTo( configurationAfter ) );
    }

    @Test
    public void toArgsThenFromArgsShouldReturnSameResultWhenAllParamsThatCanBeEmptyAreNotEmpty()
            throws DriverConfigurationException
    {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 2;

        Set<String> peerIds = Sets.newHashSet( "peer1", "peer2" );
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParamOptions =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions( "file", 2 );

        Map<String,String> paramsFromPublicStaticDefaultValuesAsMap = new HashMap<>();
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.THREADS_ARG,
                ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG,
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG,
                ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.NAME_ARG, ConsoleAndFileDriverConfiguration.NAME_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.PEER_IDS_ARG,
                ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(
                        peerIds ) );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                validationParamOptions.toCommandlineString() );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG,
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.HELP_ARG, ConsoleAndFileDriverConfiguration.HELP_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG,
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG,
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_DEFAULT_STRING );
        // required params
        paramsFromPublicStaticDefaultValuesAsMap.put( ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName );
        paramsFromPublicStaticDefaultValuesAsMap
                .put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName );
        paramsFromPublicStaticDefaultValuesAsMap
                .put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( operationCount ) );

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromParamsMap( paramsFromPublicStaticDefaultValuesAsMap );

        ConsoleAndFileDriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromArgs( configurationBefore.toArgs() );

        assertThat( configurationBefore, equalTo( configurationAfter ) );
    }

    @Test
    public void toConfigurationPropertiesStringMethodShouldOutputValidConfigurationFile()
            throws DriverConfigurationException, IOException
    {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 100;

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );

        Properties configurationProperties = new Properties();
        configurationProperties.load( new ByteArrayInputStream( configurationBefore.toPropertiesString().getBytes() ) );
        DriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration
                        .fromParamsMap( MapUtils.<String,String>propertiesToMap( configurationProperties ) );

        assertThat( configurationBefore, equalTo( configurationAfter ) );
    }

    @Test
    public void fromDefaultsWithoutChecksShouldNotFailIfRequiredAreNotProvided() throws DriverConfigurationException
    {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 0;
        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );
        assertThat( configuration.dbClassName(), is( nullValue() ) );
        assertThat( configuration.workloadClassName(), is( nullValue() ) );
        assertThat( configuration.operationCount(), is( 0l ) );
    }

    @Test
    public void fromDefaultsShouldNotFailWhenRequiredAreNotProvided()
    {
        DriverConfiguration configuration = null;
        boolean exceptionThrown = false;
        try
        {
            String databaseClassName = null;
            String workloadClassName = null;
            long operationCount = 0;
            configuration = ConsoleAndFileDriverConfiguration
                    .fromDefaults( databaseClassName, workloadClassName, operationCount );
        }
        catch ( DriverConfigurationException e )
        {
            exceptionThrown = true;
        }
        assertThat( exceptionThrown, is( false ) );
        assertThat( configuration, is( notNullValue() ) );
        assertThat( configuration.dbClassName(), is( nullValue() ) );
        assertThat( configuration.workloadClassName(), is( nullValue() ) );
        assertThat( configuration.operationCount(), is( 0l ) );
    }

    @Test
    public void fromDefaultsAndFromPublicStaticDefaultValuesAndFromDefaultParamsMapShouldAllBeEqual()
            throws DriverConfigurationException
    {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 2;

        Map<String,String> paramsFromPublicStaticDefaultValuesAsMap = new HashMap<>();
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.THREADS_ARG,
                ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG,
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG,
                ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.NAME_ARG, ConsoleAndFileDriverConfiguration.NAME_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put( ConsoleAndFileDriverConfiguration.PEER_IDS_ARG,
                ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT_STRING );
        if ( null != ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT )
        {
            paramsFromPublicStaticDefaultValuesAsMap.put(
                    ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                    ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT.toCommandlineString() );
        }
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG,
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.HELP_ARG, ConsoleAndFileDriverConfiguration.HELP_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG,
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG,
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_DEFAULT_STRING );
        // add required params
        paramsFromPublicStaticDefaultValuesAsMap.put( ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName );
        paramsFromPublicStaticDefaultValuesAsMap
                .put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName );
        paramsFromPublicStaticDefaultValuesAsMap
                .put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( operationCount ) );
        DriverConfiguration configurationFromPublicStaticDefaultValuesAsMap =
                ConsoleAndFileDriverConfiguration.fromParamsMap( paramsFromPublicStaticDefaultValuesAsMap );

        DriverConfiguration configurationFromDefault = ConsoleAndFileDriverConfiguration.fromDefaults(
                databaseClassName,
                workloadClassName,
                operationCount );

        Map<String,String> defaultOptionalParamsMap = ConsoleAndFileDriverConfiguration.defaultsAsMap();
        defaultOptionalParamsMap.put( ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName );
        defaultOptionalParamsMap.put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName );
        defaultOptionalParamsMap
                .put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( operationCount ) );
        DriverConfiguration configurationFromDefaultOptionalParamsMap =
                ConsoleAndFileDriverConfiguration.fromParamsMap( defaultOptionalParamsMap );

        assertThat( configurationFromPublicStaticDefaultValuesAsMap, equalTo( configurationFromDefault ) );
        assertThat( configurationFromPublicStaticDefaultValuesAsMap,
                equalTo( configurationFromDefaultOptionalParamsMap ) );
        assertThat( configurationFromDefault, equalTo( configurationFromDefaultOptionalParamsMap ) );
    }

    @Test
    public void shouldReturnSameConfigurationUsingFromArgsAsUsingFromParamsMap() throws DriverConfigurationException
    {
        Set<String> peerIds = Sets.newHashSet( "peerId1", "peerId2" );

        // Required
        Map<String,String> requiredParamsMap = new HashMap<>();
        requiredParamsMap.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1" );
        requiredParamsMap.put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name" );
        requiredParamsMap.put( ConsoleAndFileDriverConfiguration.DB_ARG, "db class name" );
        // Optional
        Map<String,String> optionalParamsMap = new HashMap<>();
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.THREADS_ARG,
                ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG,
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG,
                ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.NAME_ARG,
                ConsoleAndFileDriverConfiguration.NAME_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.PEER_IDS_ARG,
                ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline( peerIds ) );
        if ( null != ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT )
        {
            optionalParamsMap.put( ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                    ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT.toCommandlineString() );
        }
        if ( null != ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING )
        {
            optionalParamsMap.put( ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                    ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
        }
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG,
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.HELP_ARG,
                ConsoleAndFileDriverConfiguration.HELP_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG,
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING );
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG,
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_DEFAULT_STRING );
        // Extra
        optionalParamsMap.put( "extra_key", "extra_value" );

        // Required
        List<String> requiredParamsArgsList = new ArrayList<>();
        requiredParamsArgsList
                .addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1" ) );
        requiredParamsArgsList.addAll(
                Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name" ) );
        requiredParamsArgsList
                .addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.DB_ARG, "db class name" ) );
        // Optional
        List<String> optionalParamsArgsList = new ArrayList<>();
        optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.THREADS_ARG,
                ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING ) );
        optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG,
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING ) );
        optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG,
                ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING ) );
        if ( null != ConsoleAndFileDriverConfiguration.NAME_ARG )
        {
            optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.NAME_ARG,
                    ConsoleAndFileDriverConfiguration.NAME_DEFAULT_STRING ) );
        }
        if ( null != ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT )
        {
            optionalParamsArgsList.addAll(
                    Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_ARG,
                            ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT_STRING ) );
        }
        optionalParamsArgsList.addAll(
                Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                        ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING ) );
        optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.PEER_IDS_ARG,
                ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline( peerIds ) ) );
        if ( null != ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT )
        {
            optionalParamsArgsList.addAll(
                    Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG,
                            ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT
                                    .toCommandlineString() ) );
        }
        if ( null != ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING )
        {
            optionalParamsArgsList.addAll(
                    Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                            ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING ) );
        }
        if ( ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT )
        {
            optionalParamsArgsList.addAll(
                    Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG ) );
        }
        if ( ConsoleAndFileDriverConfiguration.HELP_DEFAULT )
        { optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.HELP_ARG ) ); }
        if ( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_DEFAULT )
        {
            optionalParamsArgsList.addAll(
                    Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG ) );
        }
        optionalParamsArgsList.addAll(
                Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG,
                        ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING ) );
        optionalParamsArgsList.addAll( Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG,
                ConsoleAndFileDriverConfiguration.WARMUP_COUNT_DEFAULT_STRING ) );
        // Extra
        optionalParamsArgsList.addAll( Lists.newArrayList( "-p", "extra_key", "extra_value" ) );

        // When
        Map<String,String> paramsMap = MapUtils.mergeMaps( requiredParamsMap, optionalParamsMap, false );
        DriverConfiguration configurationFromParamsMap = ConsoleAndFileDriverConfiguration.fromParamsMap( paramsMap );

        String[] requiredParamsArgs = requiredParamsArgsList.toArray( new String[requiredParamsArgsList.size()] );
        String[] optionalParamsArgs = optionalParamsArgsList.toArray( new String[optionalParamsArgsList.size()] );
        System.arraycopy( requiredParamsArgs, 0, optionalParamsArgs, 0, requiredParamsArgs.length );
        DriverConfiguration configurationFromParamsArgs =
                ConsoleAndFileDriverConfiguration.fromArgs( optionalParamsArgs );

        // Then
        assertThat( configurationFromParamsMap, equalTo( configurationFromParamsArgs ) );
        assertThat( configurationFromParamsMap.asMap(), equalTo( configurationFromParamsArgs.asMap() ) );
    }

    @Test
    public void
    shouldWorkWhenOnlyRequiredParametersAreGivenAndAssignCorrectDefaultsForOptionalParametersThatAreNotProvided()
            throws DriverConfigurationException
    {
        // Given
        Map<String,String> requiredParams = new HashMap<>();
        requiredParams.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1" );
        requiredParams.put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name" );
        requiredParams.put( ConsoleAndFileDriverConfiguration.DB_ARG, "db class name" );

        // When
        ConsoleAndFileDriverConfiguration configurationFromParams =
                ConsoleAndFileDriverConfiguration.fromParamsMap( requiredParams );

        // Then
        assertThat( configurationFromParams.name(), equalTo( ConsoleAndFileDriverConfiguration.NAME_DEFAULT ) );
        assertThat( configurationFromParams.dbClassName(), equalTo( "db class name" ) );
        assertThat( configurationFromParams.workloadClassName(), equalTo( "workload class name" ) );
        assertThat( configurationFromParams.operationCount(), is( 1l ) );
        assertThat( configurationFromParams.threadCount(), is( ConsoleAndFileDriverConfiguration.THREADS_DEFAULT ) );
        assertThat( configurationFromParams.statusDisplayIntervalAsSeconds(),
                is( ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT ) );
        assertThat( configurationFromParams.timeUnit(), is( ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT ) );
        assertThat( new File( configurationFromParams.resultDirPath() ).getName(),
                is( ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT ) );
        assertThat( configurationFromParams.timeCompressionRatio(),
                is( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT ) );
        assertThat( configurationFromParams.peerIds(), is( ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT ) );
        assertThat( configurationFromParams.validationParamsCreationOptions(),
                is( (DriverConfiguration.ValidationParamOptions) ConsoleAndFileDriverConfiguration
                        .CREATE_VALIDATION_PARAMS_DEFAULT ) );
        assertThat( configurationFromParams.databaseValidationFilePath(),
                is( ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT ) );
        assertThat( configurationFromParams.calculateWorkloadStatistics(),
                is( ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT ) );
        assertThat( configurationFromParams.shouldPrintHelpString(),
                is( ConsoleAndFileDriverConfiguration.HELP_DEFAULT ) );
        assertThat( configurationFromParams.ignoreScheduledStartTimes(),
                is( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_DEFAULT ) );
        assertThat( configurationFromParams.spinnerSleepDurationAsMilli(),
                is( ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT ) );
        assertThat( configurationFromParams.warmupCount(),
                is( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_DEFAULT ) );
    }

    @Test
    public void shouldReturnSameAsConstructedWith()
    {
        Map<String,String> paramsMap = new HashMap<>();
        String name = "name";
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        int threadCount = 3;
        int statusDisplayInterval = 1000;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        Set<String> peerIds = Sets.newHashSet( "1" );
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions( "file", 1 );
        String dbValidationFilePath = null;
        boolean calculateWorkloadStatistics = false;
        long spinnerSleepDuration = 0l;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 10;
        long skipCount = 100;

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                peerIds,
                validationParams,
                dbValidationFilePath,
                calculateWorkloadStatistics,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount
        );

        assertThat( params.asMap(), equalTo( paramsMap ) );
        assertThat( params.name(), equalTo( name ) );
        assertThat( params.dbClassName(), equalTo( dbClassName ) );
        assertThat( params.workloadClassName(), equalTo( workloadClassName ) );
        assertThat( params.operationCount(), equalTo( operationCount ) );
        assertThat( params.threadCount(), equalTo( threadCount ) );
        assertThat( params.statusDisplayIntervalAsSeconds(), equalTo( statusDisplayInterval ) );
        assertThat( params.timeUnit(), equalTo( timeUnit ) );
        assertThat( params.resultDirPath(), equalTo( resultDirPath ) );
        assertThat( params.timeCompressionRatio(), equalTo( timeCompressionRatio ) );
        assertThat( params.peerIds(), equalTo( peerIds ) );
        assertThat( params.validationParamsCreationOptions(),
                equalTo( (DriverConfiguration.ValidationParamOptions) validationParams ) );
        assertThat( params.databaseValidationFilePath(), equalTo( dbValidationFilePath ) );
        assertThat( params.calculateWorkloadStatistics(), equalTo( calculateWorkloadStatistics ) );
        assertThat( params.shouldPrintHelpString(), equalTo( printHelp ) );
        assertThat( params.ignoreScheduledStartTimes(), equalTo( ignoreScheduledStartTimes ) );
        assertThat( params.spinnerSleepDurationAsMilli(), equalTo( spinnerSleepDuration ) );
    }

    @Test
    // Make sure that all tests that use test resources configuration file are using the same file as in the public
    // directory
    public void testResourcesBaseConfigurationFileAndPublicBaseConfigurationFilesShouldBeEqual()
            throws DriverConfigurationException, IOException
    {
        File ldbcDriverConfigurationInTestResourcesFile =
                DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation();
        Properties ldbcDriverConfigurationInTestResourcesProperties = new Properties();
        ldbcDriverConfigurationInTestResourcesProperties
                .load( new FileInputStream( ldbcDriverConfigurationInTestResourcesFile ) );
        Map<String,String> configurationInTestResourcesAsMap =
                MapUtils.propertiesToMap( ldbcDriverConfigurationInTestResourcesProperties );

        File ldbcDriverConfigurationInWorkloadsDirectoryFile =
                DriverConfigurationFileHelper.getBaseConfigurationFilePublicLocation();
        Properties ldbcDriverConfigurationInWorkloadsDirectoryProperties = new Properties();
        ldbcDriverConfigurationInWorkloadsDirectoryProperties
                .load( new FileInputStream( ldbcDriverConfigurationInWorkloadsDirectoryFile ) );
        Map<String,String> configurationInWorkloadsDirectoryAsMap =
                MapUtils.propertiesToMap( ldbcDriverConfigurationInWorkloadsDirectoryProperties );

        assertThat( configurationInTestResourcesAsMap, equalTo( configurationInWorkloadsDirectoryAsMap ) );

        Map<String,String> requiredParamsAsMap = new HashMap<>();
        requiredParamsAsMap.put( ConsoleAndFileDriverConfiguration.DB_ARG, DummyLdbcSnbInteractiveDb.class.getName() );
        requiredParamsAsMap
                .put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, LdbcSnbInteractiveWorkload.class.getName() );
        requiredParamsAsMap.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString( 1000 ) );

        DriverConfiguration configurationFromTestResources =
                ConsoleAndFileDriverConfiguration.fromParamsMap(
                        MapUtils.mergeMaps( configurationInTestResourcesAsMap, requiredParamsAsMap, true ) );

        DriverConfiguration configurationFromWorkloadsDirectory =
                ConsoleAndFileDriverConfiguration.fromParamsMap(
                        MapUtils.mergeMaps( configurationInWorkloadsDirectoryAsMap, requiredParamsAsMap, true ) );

        assertThat( configurationFromTestResources, equalTo( configurationFromWorkloadsDirectory ) );
    }

    @Test
    public void shouldSerializeAndParsePeerIds()
    {
        // Given
        Set<String> peerIds0 = Sets.newHashSet();
        Set<String> peerIds1 = Sets.newHashSet( "1", "2" );
        Set<String> peerIds2 = Sets.newHashSet( "1", "2", "3" );

        // When
        String peerIdsString0 = ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline( peerIds0 );
        String peerIdsString1 = ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline( peerIds1 );
        String peerIdsString2 = ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline( peerIds2 );

        // Then
        assertThat( ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIdsString0 ),
                equalTo( peerIds0 ) );
        assertThat( ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIdsString1 ),
                equalTo( peerIds1 ) );
        assertThat( ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIdsString2 ),
                equalTo( peerIds2 ) );
    }

    @Test
    public void shouldParsePeerIds()
    {
        // Given
        String peerIds1String = "";
        String peerIds2String = "|";
        String peerIds3String = "|1";
        String peerIds4String = "|1";
        String peerIds5String = "|1|";
        String peerIds6String = "|1|2";
        String peerIds7String = "1|2|";
        String peerIds8String = "1";
        String peerIds9String = "1|2";
        String peerIds10String = "1|2|3";

        Set<String> peerIds1Expected = Sets.newHashSet();
        Set<String> peerIds2Expected = Sets.newHashSet();
        Set<String> peerIds3Expected = Sets.newHashSet( "1" );
        Set<String> peerIds4Expected = Sets.newHashSet( "1" );
        Set<String> peerIds5Expected = Sets.newHashSet( "1" );
        Set<String> peerIds6Expected = Sets.newHashSet( "1", "2" );
        Set<String> peerIds7Expected = Sets.newHashSet( "1", "2" );
        Set<String> peerIds8Expected = Sets.newHashSet( "1" );
        Set<String> peerIds9Expected = Sets.newHashSet( "1", "2" );
        Set<String> peerIds10Expected = Sets.newHashSet( "1", "2", "3" );

        // When
        Set<String> peerIds1 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds1String );
        Set<String> peerIds2 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds2String );
        Set<String> peerIds3 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds3String );
        Set<String> peerIds4 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds4String );
        Set<String> peerIds5 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds5String );
        Set<String> peerIds6 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds6String );
        Set<String> peerIds7 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds7String );
        Set<String> peerIds8 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds8String );
        Set<String> peerIds9 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds9String );
        Set<String> peerIds10 = ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline( peerIds10String );

        // Then
        assertThat( peerIds1, equalTo( peerIds1Expected ) );
        assertThat( peerIds2, equalTo( peerIds2Expected ) );
        assertThat( peerIds3, equalTo( peerIds3Expected ) );
        assertThat( peerIds4, equalTo( peerIds4Expected ) );
        assertThat( peerIds5, equalTo( peerIds5Expected ) );
        assertThat( peerIds6, equalTo( peerIds6Expected ) );
        assertThat( peerIds7, equalTo( peerIds7Expected ) );
        assertThat( peerIds8, equalTo( peerIds8Expected ) );
        assertThat( peerIds9, equalTo( peerIds9Expected ) );
        assertThat( peerIds10, equalTo( peerIds10Expected ) );
    }
}