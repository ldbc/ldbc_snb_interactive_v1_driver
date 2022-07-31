package org.ldbcouncil.snb.driver.control;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.workloads.interactive.LdbcSnbInteractiveWorkload;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveDb;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ConsoleAndFileDriverConfigurationTest
{
    @Test
    public void applyShouldWork() throws DriverConfigurationException
    {
        ConsoleAndFileDriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( "db1", "workload1", 1 );

        assertEquals("db1", configuration.dbClassName());
        assertEquals("workload1", configuration.workloadClassName());
        assertEquals(1L, configuration.operationCount());

        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.DB_ARG, "db2" );
                assertEquals("db2", configuration.dbClassName());

        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload2" );
        assertEquals("workload2", configuration.workloadClassName());

        configuration = (ConsoleAndFileDriverConfiguration) configuration
                .applyArg( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "2" );
                assertEquals(2L, configuration.operationCount());
    }

    @Test
    public void applyMapShouldWork() throws DriverConfigurationException
    {
        ConsoleAndFileDriverConfiguration configuration1 =
                ConsoleAndFileDriverConfiguration.fromDefaults( "db1", "workload1", 1 );

        assertEquals("db1", configuration1.dbClassName());
        assertEquals("workload1", configuration1.workloadClassName());
        assertEquals(1L, configuration1.operationCount());

        Map<String,String> configurationInsert2 = new HashMap<>();
        configurationInsert2.put( ConsoleAndFileDriverConfiguration.DB_ARG, "db2" );
        configurationInsert2.put( ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload2" );
        configurationInsert2.put( ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "2" );

        ConsoleAndFileDriverConfiguration configuration2 =
                (ConsoleAndFileDriverConfiguration) configuration1.applyArgs( configurationInsert2 );

        assertEquals("db2", configuration2.dbClassName());
        assertEquals("workload2", configuration2.workloadClassName());
        assertEquals(2L, configuration2.operationCount());
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

        assertEquals( configurationBefore, configurationAfter );
    }

    @Test
    public void toMapThenFromMapShouldReturnSameResultWhenAllParamsAreInitiallySetViaConstructor()
            throws DriverConfigurationException
    {
        String mode = "execute_benchmark";
        long operationCount = 2;
        int threadCount = 4;
        int statusDisplayInterval = 1000;
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        String resultDirPath = "results dir";
        Double timeCompressionRatio = 1.0;
        int validationParamsSize = 0;
        boolean validationSerializationCheck = false;
        boolean recordDelayedOperations = true;
        String databaseValidationFilePath = null;
        long spinnerSleepDuration = 0L;
        boolean printHelp = false;
        String name = "LDBC-SNB";
        boolean ignoreScheduledStartTimes = true;
        long warmupCount = 5;
        long skipCount = 6;
        Map<String,String> paramsMap = new HashMap<>();
        boolean flushLog = false;

        ConsoleAndFileDriverConfiguration configurationBefore = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                mode,
                name,
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                validationParamsSize,
                validationSerializationCheck,
                recordDelayedOperations,
                databaseValidationFilePath,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount,
                flushLog
        );

        DriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromParamsMap( configurationBefore.asMap() );

        assertEquals( configurationBefore, configurationAfter );
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

        assertEquals( configurationBefore, configurationAfter );
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

        assertEquals( configurationBefore, configurationAfter );
    }

    @Test
    public void toArgsThenFromArgsShouldReturnSameResultWhenAllParamsThatCanBeEmptyAreNotEmpty()
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
        paramsFromPublicStaticDefaultValuesAsMap.put(
            ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_ARG,
            ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
            ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_ARG,
            ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
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

        assertEquals( configurationBefore, configurationAfter );
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

        assertEquals( configurationBefore, configurationAfter );
    }

    @Test
    public void fromDefaultsWithoutChecksShouldNotFailIfRequiredAreNotProvided() throws DriverConfigurationException
    {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 0;
        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaults( databaseClassName, workloadClassName, operationCount );
        assertNull( configuration.dbClassName() );
        assertNull( configuration.workloadClassName() );
        assertEquals( 0L, configuration.operationCount());
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
        assertFalse( exceptionThrown );
        assertNotNull( configuration );
        assertNull( configuration.dbClassName() );
        assertNull( configuration.workloadClassName() );
        assertEquals(0L, configuration.operationCount() );
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
        paramsFromPublicStaticDefaultValuesAsMap.put(
            ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_ARG,
            ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
            ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_ARG,
            ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_DEFAULT_STRING );
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
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

        assertEquals( configurationFromPublicStaticDefaultValuesAsMap, configurationFromDefault );
        assertEquals( configurationFromPublicStaticDefaultValuesAsMap, configurationFromDefaultOptionalParamsMap );
        assertEquals( configurationFromDefault, configurationFromDefaultOptionalParamsMap );
    }

    @Test
    public void shouldReturnSameConfigurationUsingFromArgsAsUsingFromParamsMap() throws DriverConfigurationException
    {
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

        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_ARG,
            ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_DEFAULT_STRING);
        
        optionalParamsMap.put( ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_ARG,
            ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_DEFAULT_STRING);

        if ( null != ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING )
        {
            optionalParamsMap.put( ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                    ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING );
        }
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
        
        optionalParamsArgsList.addAll(
            Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_ARG,
                ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_DEFAULT_STRING ) );

        if (ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_DEFAULT == false){
            optionalParamsArgsList.addAll(
                Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_ARG,
                        ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_DEFAULT_STRING ) );
        }

        if ( null != ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING )
        {
            optionalParamsArgsList.addAll(
                    Lists.newArrayList( "-" + ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG,
                            ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING ) );
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
        assertEquals( configurationFromParamsMap,  configurationFromParamsArgs );
        assertEquals( configurationFromParamsMap.asMap(),  configurationFromParamsArgs.asMap() );
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
        assertEquals( ConsoleAndFileDriverConfiguration.NAME_DEFAULT, configurationFromParams.name()   );
        assertEquals( "db class name" , configurationFromParams.dbClassName()  );
        assertEquals( "workload class name", configurationFromParams.workloadClassName() );
        assertEquals( 1L, configurationFromParams.operationCount() );
        assertEquals( ConsoleAndFileDriverConfiguration.THREADS_DEFAULT, configurationFromParams.threadCount() );
        assertEquals( ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT, configurationFromParams.statusDisplayIntervalAsSeconds());
        assertEquals( ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT, configurationFromParams.timeUnit() );
        assertEquals( ConsoleAndFileDriverConfiguration.RESULT_DIR_PATH_DEFAULT, new File( configurationFromParams.resultDirPath() ).getName());
        assertEquals( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT, configurationFromParams.timeCompressionRatio());
        assertEquals( ConsoleAndFileDriverConfiguration.VALIDATION_PARAMS_SIZE_DEFAULT, configurationFromParams.validationParametersSize());
        assertEquals( ConsoleAndFileDriverConfiguration.VALIDATION_SERIALIZATION_CHECK_DEFAULT, configurationFromParams.validationSerializationCheck());
        assertEquals( ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT, configurationFromParams.databaseValidationFilePath());
        assertEquals( ConsoleAndFileDriverConfiguration.HELP_DEFAULT, configurationFromParams.shouldPrintHelpString());
        assertEquals( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_DEFAULT, configurationFromParams.ignoreScheduledStartTimes());
        assertEquals( ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT, configurationFromParams.spinnerSleepDurationAsMilli());
        assertEquals( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_DEFAULT, configurationFromParams.warmupCount());
    }

    @Test
    public void shouldReturnSameAsConstructedWith()
    {
        Map<String,String> paramsMap = new HashMap<>();
        String mode = "execute_benchmark";
        String name = "name";
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        int threadCount = 3;
        int statusDisplayInterval = 1000;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        String resultDirPath = null;
        Double timeCompressionRatio = 1.0;
        int validationParamsSize = 1;
        boolean validationSerializationCheck = true;
        boolean recordDelayedOperations = true;
        String dbValidationFilePath = "file";
        long spinnerSleepDuration = 0L;
        boolean printHelp = false;
        boolean ignoreScheduledStartTimes = false;
        long warmupCount = 10;
        long skipCount = 100;
        boolean flushLog = false;

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                mode,
                name,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                statusDisplayInterval,
                timeUnit,
                resultDirPath,
                timeCompressionRatio,
                validationParamsSize,
                validationSerializationCheck,
                recordDelayedOperations,
                dbValidationFilePath,
                spinnerSleepDuration,
                printHelp,
                ignoreScheduledStartTimes,
                warmupCount,
                skipCount,
                flushLog
        );

        assertEquals( paramsMap, params.asMap());
        assertEquals( mode, params.mode() );
        assertEquals( name, params.name() );
        assertEquals( dbClassName, params.dbClassName() );
        assertEquals( workloadClassName, params.workloadClassName() );
        assertEquals( operationCount, params.operationCount() );
        assertEquals( threadCount, params.threadCount() );
        assertEquals( statusDisplayInterval, params.statusDisplayIntervalAsSeconds() );
        assertEquals( timeUnit, params.timeUnit() );
        assertEquals( resultDirPath, params.resultDirPath() );
        assertEquals( timeCompressionRatio, params.timeCompressionRatio() );
        assertEquals( validationSerializationCheck, params.validationSerializationCheck() );
        assertEquals( validationParamsSize, params.validationParametersSize() );
        assertEquals( dbValidationFilePath, params.databaseValidationFilePath() );
        assertEquals( printHelp, params.shouldPrintHelpString() );
        assertEquals( ignoreScheduledStartTimes, params.ignoreScheduledStartTimes() );
        assertEquals( spinnerSleepDuration, params.spinnerSleepDurationAsMilli() );
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

        assertEquals( configurationInWorkloadsDirectoryAsMap, configurationInTestResourcesAsMap );

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

        assertEquals( configurationFromWorkloadsDirectory, configurationFromTestResources);
    }
}
