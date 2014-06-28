package com.ldbc.driver.control;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcSnbInteractiveWorkload;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyDb;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

public class ConsoleAndFileDriverConfigurationTests {

    @Test
    public void addTestForApplyMap() throws DriverConfigurationException {
        ConsoleAndFileDriverConfiguration configuration1 = ConsoleAndFileDriverConfiguration.fromDefaults("db1", "workload1", 1);

        assertThat(configuration1.dbClassName(), equalTo("db1"));
        assertThat(configuration1.workloadClassName(), equalTo("workload1"));
        assertThat(configuration1.operationCount(), equalTo(1l));

        Map<String, String> configurationUpdate2 = new HashMap<>();
        configurationUpdate2.put(ConsoleAndFileDriverConfiguration.DB_ARG, "db2");
        configurationUpdate2.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload2");
        configurationUpdate2.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "2");

        ConsoleAndFileDriverConfiguration configuration2 = (ConsoleAndFileDriverConfiguration) configuration1.applyMap(configurationUpdate2);

        assertThat(configuration2.dbClassName(), equalTo("db2"));
        assertThat(configuration2.workloadClassName(), equalTo("workload2"));
        assertThat(configuration2.operationCount(), equalTo(2l));
    }

    @Ignore
    @Test
    public void addWindowSizeParameter() {
        assertThat(true, is(false));
    }

    @Ignore
    @Test
    public void considerAddingApplyMapsAsInPluralMapsForConvenience() {
        assertThat(true, is(false));
    }

    @Test
    public void toArgsThenFromArgsShouldFailWhenRequiredParametersAreNotSet() throws DriverConfigurationException {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 2;

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromDefaultsWithoutChecks(databaseClassName, workloadClassName, operationCount);

        boolean exceptionThrown = false;
        try {
            ConsoleAndFileDriverConfiguration.fromArgs(configurationBefore.toArgs());
        } catch (DriverConfigurationException e) {
            exceptionThrown = true;
        }

        assertThat(exceptionThrown, equalTo(true));
    }

    @Test
    public void toArgsThenFromArgsShouldReturnSameResultWhenAllParamsThatCanBeEmptyAreEmpty() throws DriverConfigurationException {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 2;

        Map<String, String> paramsFromPublicStaticDefaultValuesAsMap = new HashMap<>();
        // required params
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName);
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName);
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(operationCount));

        ConsoleAndFileDriverConfiguration configurationBefore = ConsoleAndFileDriverConfiguration.fromParamsMap(paramsFromPublicStaticDefaultValuesAsMap);

        ConsoleAndFileDriverConfiguration configurationAfter = ConsoleAndFileDriverConfiguration.fromArgs(configurationBefore.toArgs());

        assertThat(configurationBefore, equalTo(configurationAfter));
    }

    @Test
    public void toArgsThenFromArgsShouldReturnSameResultWhenAllParamsThatCanBeEmptyAreNotEmpty() throws DriverConfigurationException {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 2;

        Set<String> peerIds = Sets.newHashSet("peer1", "peer2");
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParamOptions =
                new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions("file", 2);

        Map<String, String> paramsFromPublicStaticDefaultValuesAsMap = new HashMap<>();
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.THREADS_ARG, ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG, ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_ARG, ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.PEER_IDS_ARG, ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds));
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, validationParamOptions.toCommandlineString());
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_ARG, ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG, ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG, ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING);
        // required params
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName);
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName);
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(operationCount));

        ConsoleAndFileDriverConfiguration configurationBefore = ConsoleAndFileDriverConfiguration.fromParamsMap(paramsFromPublicStaticDefaultValuesAsMap);

        ConsoleAndFileDriverConfiguration configurationAfter = ConsoleAndFileDriverConfiguration.fromArgs(configurationBefore.toArgs());

        assertThat(configurationBefore, equalTo(configurationAfter));
    }

    @Test
    public void toConfigurationPropertiesStringMethodShouldOutputValidConfigurationFile() throws DriverConfigurationException, IOException {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 100;

        ConsoleAndFileDriverConfiguration configurationBefore =
                ConsoleAndFileDriverConfiguration.fromDefaults(databaseClassName, workloadClassName, operationCount);

        Properties configurationProperties = new Properties();
        configurationProperties.load(new ByteArrayInputStream(configurationBefore.toPropertiesString().getBytes()));
        DriverConfiguration configurationAfter =
                ConsoleAndFileDriverConfiguration.fromParamsMap(MapUtils.<String, String>propertiesToMap(configurationProperties));

        assertThat(configurationBefore, equalTo(configurationAfter));
    }

    @Test
    public void fromDefaultsWithoutChecksShouldNotFailIfRequiredAreNotProvided() throws DriverConfigurationException {
        String databaseClassName = null;
        String workloadClassName = null;
        long operationCount = 0;
        DriverConfiguration configuration =
                ConsoleAndFileDriverConfiguration.fromDefaultsWithoutChecks(databaseClassName, workloadClassName, operationCount);
        assertThat(configuration.dbClassName(), is(nullValue()));
        assertThat(configuration.workloadClassName(), is(nullValue()));
        assertThat(configuration.operationCount(), is(0l));
    }

    @Test
    public void fromDefaultsShouldFailIfRequiredAreNotProvided() {
        boolean exceptionThrown = false;
        try {
            String databaseClassName = null;
            String workloadClassName = null;
            long operationCount = 0;
            ConsoleAndFileDriverConfiguration.fromDefaults(databaseClassName, workloadClassName, operationCount);
        } catch (DriverConfigurationException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void fromDefaultsAndFromPublicStaticDefaultValuesAndFromDefaultParamsMapShouldAllBeEqual() throws DriverConfigurationException {
        String databaseClassName = "db";
        String workloadClassName = "workload";
        long operationCount = 2;

        Map<String, String> paramsFromPublicStaticDefaultValuesAsMap = new HashMap<>();
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.THREADS_ARG, ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG, ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_ARG, ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT_STRING);
        if (null != ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT && false == ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT.isEmpty())
            paramsFromPublicStaticDefaultValuesAsMap.put(
                    ConsoleAndFileDriverConfiguration.PEER_IDS_ARG, ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT));
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT_STRING);
        if (null != ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT)
            paramsFromPublicStaticDefaultValuesAsMap.put(
                    ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT.toCommandlineString());
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_ARG, ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG, ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING);
        paramsFromPublicStaticDefaultValuesAsMap.put(
                ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG, ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING);
        // add required params
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName);
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName);
        paramsFromPublicStaticDefaultValuesAsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(operationCount));
        DriverConfiguration configurationFromPublicStaticDefaultValuesAsMap =
                ConsoleAndFileDriverConfiguration.fromParamsMap(paramsFromPublicStaticDefaultValuesAsMap);

        DriverConfiguration configurationFromDefault = ConsoleAndFileDriverConfiguration.fromDefaults(
                databaseClassName,
                workloadClassName,
                operationCount);

        Map<String, String> defaultOptionalParamsMap = ConsoleAndFileDriverConfiguration.defaultsForOptionalParamsAsMap();
        defaultOptionalParamsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, databaseClassName);
        defaultOptionalParamsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, workloadClassName);
        defaultOptionalParamsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(operationCount));
        DriverConfiguration configurationFromDefaultOptionalParamsMap =
                ConsoleAndFileDriverConfiguration.fromParamsMap(defaultOptionalParamsMap);

        assertThat(configurationFromPublicStaticDefaultValuesAsMap, equalTo(configurationFromDefault));
        assertThat(configurationFromPublicStaticDefaultValuesAsMap, equalTo(configurationFromDefaultOptionalParamsMap));
        assertThat(configurationFromDefault, equalTo(configurationFromDefaultOptionalParamsMap));
    }

    @Test
    public void shouldReturnSameConfigurationFromFromArgsAsFromFromParamsMap() throws DriverConfigurationException {
        Set<String> peerIds = Sets.newHashSet("peerId1", "peerId2");

        // Required
        Map<String, String> requiredParamsMap = new HashMap<>();
        requiredParamsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1");
        requiredParamsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name");
        requiredParamsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, "db class name");
        // Optional
        Map<String, String> optionalParamsMap = new HashMap<>();
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.THREADS_ARG, ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG, ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_ARG, ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.PEER_IDS_ARG, ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds));
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT_STRING);
        if (null != ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT)
            optionalParamsMap.put(ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT.toCommandlineString());
        if (null != ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING)
            optionalParamsMap.put(ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING);
        if (null != ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT_STRING)
            optionalParamsMap.put(ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_ARG, ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG, ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT_STRING);
        optionalParamsMap.put(ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG, ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING);
        // Extra
        optionalParamsMap.put("extra_key", "extra_value");

        // Required
        List<String> requiredParamsArgsList = new ArrayList<>();
        requiredParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1"));
        requiredParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name"));
        requiredParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.DB_ARG, "db class name"));
        // Optional
        List<String> optionalParamsArgsList = new ArrayList<>();
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.THREADS_ARG, ConsoleAndFileDriverConfiguration.THREADS_DEFAULT_STRING));
        if (ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.SHOW_STATUS_ARG));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.TIME_UNIT_ARG, ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT_STRING));
        if (null != ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT_STRING));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT_STRING));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_ARG, ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT_STRING));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.PEER_IDS_ARG, ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds)));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_ARG, ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT_STRING));
        if (null != ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_ARG, ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT.toCommandlineString()));
        if (null != ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_ARG, ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT_STRING));
        if (ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_ARG));
        if (ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT)
            optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_ARG));
        optionalParamsArgsList.addAll(Lists.newArrayList("-" + ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_ARG, ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT_STRING));
        // Extra
        optionalParamsArgsList.addAll(Lists.newArrayList("-p", "extra_key", "extra_value"));

        // When
        Map<String, String> paramsMap = MapUtils.mergeMaps(requiredParamsMap, optionalParamsMap, false);
        DriverConfiguration configurationFromParamsMap = ConsoleAndFileDriverConfiguration.fromParamsMap(paramsMap);

        String[] requiredParamsArgs = requiredParamsArgsList.toArray(new String[requiredParamsArgsList.size()]);
        String[] optionalParamsArgs = optionalParamsArgsList.toArray(new String[optionalParamsArgsList.size()]);
        System.arraycopy(requiredParamsArgs, 0, optionalParamsArgs, 0, requiredParamsArgs.length);
        DriverConfiguration configurationFromParamsArgs = ConsoleAndFileDriverConfiguration.fromArgs(optionalParamsArgs);

        // Then
        assertThat(configurationFromParamsMap, equalTo(configurationFromParamsArgs));
        assertThat(configurationFromParamsMap.asMap(), equalTo(configurationFromParamsArgs.asMap()));
    }

    @Test
    public void shouldWorkWhenOnlyRequiredParametersAreGivenAndAssignCorrectDefaultsForOptionalParametersThatAreNotProvided() throws DriverConfigurationException {
        // Given
        Map<String, String> requiredParams = new HashMap<>();
        requiredParams.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, "1");
        requiredParams.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, "workload class name");
        requiredParams.put(ConsoleAndFileDriverConfiguration.DB_ARG, "db class name");

        // When
        ConsoleAndFileDriverConfiguration configurationFromParams = ConsoleAndFileDriverConfiguration.fromParamsMap(requiredParams);

        // Then
        assertThat(configurationFromParams.dbClassName(), equalTo("db class name"));
        assertThat(configurationFromParams.workloadClassName(), equalTo("workload class name"));
        assertThat(configurationFromParams.operationCount(), is(1l));
        assertThat(configurationFromParams.threadCount(), is(ConsoleAndFileDriverConfiguration.THREADS_DEFAULT));
        assertThat(configurationFromParams.showStatus(), is(ConsoleAndFileDriverConfiguration.SHOW_STATUS_DEFAULT));
        assertThat(configurationFromParams.timeUnit(), is(ConsoleAndFileDriverConfiguration.TIME_UNIT_DEFAULT));
        assertThat(new File(configurationFromParams.resultFilePath()).getName(), is(ConsoleAndFileDriverConfiguration.RESULT_FILE_PATH_DEFAULT));
        assertThat(configurationFromParams.timeCompressionRatio(), is(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT));
        assertThat(configurationFromParams.gctDeltaDuration(), is(ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT));
        assertThat(configurationFromParams.peerIds(), is(ConsoleAndFileDriverConfiguration.PEER_IDS_DEFAULT));
        assertThat(configurationFromParams.toleratedExecutionDelay(), is(ConsoleAndFileDriverConfiguration.TOLERATED_EXECUTION_DELAY_DEFAULT));
        assertThat(configurationFromParams.validationCreationParams(), is((DriverConfiguration.ValidationParamOptions) ConsoleAndFileDriverConfiguration.CREATE_VALIDATION_PARAMS_DEFAULT));
        assertThat(configurationFromParams.databaseValidationFilePath(), is(ConsoleAndFileDriverConfiguration.DB_VALIDATION_FILE_PATH_DEFAULT));
        assertThat(configurationFromParams.validateWorkload(), is(ConsoleAndFileDriverConfiguration.VALIDATE_WORKLOAD_DEFAULT));
        assertThat(configurationFromParams.calculateWorkloadStatistics(), is(ConsoleAndFileDriverConfiguration.CALCULATE_WORKLOAD_STATISTICS_DEFAULT));
        assertThat(configurationFromParams.compressedGctDeltaDuration(),
                is(Duration.fromMilli(Math.round(ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_DEFAULT * ConsoleAndFileDriverConfiguration.GCT_DELTA_DURATION_DEFAULT.asMilli()))));
        assertThat(configurationFromParams.spinnerSleepDuration(), is(ConsoleAndFileDriverConfiguration.SPINNER_SLEEP_DURATION_DEFAULT));
    }

    @Test
    public void shouldReturnSameAsConstructedWith() {
        Map<String, String> paramsMap = new HashMap<>();
        String dbClassName = "dbClassName";
        String workloadClassName = "workloadClassName";
        long operationCount = 1;
        int threadCount = 3;
        boolean showStatus = true;
        TimeUnit timeUnit = TimeUnit.SECONDS;
        String resultFilePath = null;
        Double timeCompressionRatio = 1.0;
        Duration gctDeltaDuration = Duration.fromMilli(1);
        Set<String> peerIds = Sets.newHashSet("1");
        Duration toleratedExecutionDelay = Duration.fromMilli(2);
        ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions validationParams = new ConsoleAndFileDriverConfiguration.ConsoleAndFileValidationParamOptions("file", 1);
        String dbValidationFilePath = null;
        boolean validateWorkload = false;
        boolean calculateWorkloadStatistics = false;
        Duration spinnerSleepDuration = Duration.fromMilli(0);

        ConsoleAndFileDriverConfiguration params = new ConsoleAndFileDriverConfiguration(
                paramsMap,
                dbClassName,
                workloadClassName,
                operationCount,
                threadCount,
                showStatus,
                timeUnit,
                resultFilePath,
                timeCompressionRatio,
                gctDeltaDuration,
                peerIds,
                toleratedExecutionDelay,
                validationParams,
                dbValidationFilePath,
                validateWorkload,
                calculateWorkloadStatistics,
                spinnerSleepDuration);

        assertThat(params.asMap(), equalTo(paramsMap));
        assertThat(params.dbClassName(), equalTo(dbClassName));
        assertThat(params.workloadClassName(), equalTo(workloadClassName));
        assertThat(params.operationCount(), equalTo(operationCount));
        assertThat(params.threadCount(), equalTo(threadCount));
        assertThat(params.showStatus(), equalTo(showStatus));
        assertThat(params.timeUnit(), equalTo(timeUnit));
        assertThat(params.resultFilePath(), equalTo(resultFilePath));
        assertThat(params.timeCompressionRatio(), equalTo(timeCompressionRatio));
        assertThat(params.gctDeltaDuration(), equalTo(gctDeltaDuration));
        assertThat(params.peerIds(), equalTo(peerIds));
        assertThat(params.toleratedExecutionDelay(), equalTo(toleratedExecutionDelay));
        assertThat(params.validationCreationParams(), equalTo((DriverConfiguration.ValidationParamOptions) validationParams));
        assertThat(params.databaseValidationFilePath(), equalTo(dbValidationFilePath));
        assertThat(params.validateWorkload(), equalTo(validateWorkload));
        assertThat(params.calculateWorkloadStatistics(), equalTo(calculateWorkloadStatistics));
        assertThat(params.spinnerSleepDuration(), equalTo(spinnerSleepDuration));
    }

    @Test
    // Make sure that all tests that use test resources configuration file are using the same file as in the public directory
    public void testResourcesBaseConfigurationFileAndPublicBaseConfigurationFilesShouldBeEqual() throws DriverConfigurationException, IOException {
        File ldbcDriverConfigurationInTestResourcesFile =
                TestUtils.getResource("/ldbc_driver_default.properties");
        Properties ldbcDriverConfigurationInTestResourcesProperties = new Properties();
        ldbcDriverConfigurationInTestResourcesProperties.load(new FileInputStream(ldbcDriverConfigurationInTestResourcesFile));
        Map<String, String> configurationInTestResourcesAsMap =
                MapUtils.propertiesToMap(ldbcDriverConfigurationInTestResourcesProperties);

        boolean exceptionThrownCreatingConfigurationFromPropertiesInTestResources = false;
        try {
            ConsoleAndFileDriverConfiguration.fromParamsMap(configurationInTestResourcesAsMap);
        } catch (DriverConfigurationException e) {
            exceptionThrownCreatingConfigurationFromPropertiesInTestResources = true;
        }
        assertThat(exceptionThrownCreatingConfigurationFromPropertiesInTestResources, is(true));

        File ldbcDriverConfigurationInWorkloadsDirectoryFile =
                ConfigurationFileCreatorReaderHelper.getBaseConfigurationFilePublicLocation();
        Properties ldbcDriverConfigurationInWorkloadsDirectoryProperties = new Properties();
        ldbcDriverConfigurationInWorkloadsDirectoryProperties.load(new FileInputStream(ldbcDriverConfigurationInWorkloadsDirectoryFile));
        Map<String, String> configurationInWorkloadsDirectoryAsMap =
                MapUtils.propertiesToMap(ldbcDriverConfigurationInWorkloadsDirectoryProperties);

        boolean exceptionThrownCreatingConfigurationFromPropertiesInWorkloadsDirectory = false;
        try {
            ConsoleAndFileDriverConfiguration.fromParamsMap(configurationInWorkloadsDirectoryAsMap);
        } catch (DriverConfigurationException e) {
            exceptionThrownCreatingConfigurationFromPropertiesInWorkloadsDirectory = true;
        }
        assertThat(exceptionThrownCreatingConfigurationFromPropertiesInWorkloadsDirectory, is(true));

        assertThat(configurationInTestResourcesAsMap, equalTo(configurationInWorkloadsDirectoryAsMap));

        Map<String, String> requiredParamsAsMap = new HashMap<>();
        requiredParamsAsMap.put(ConsoleAndFileDriverConfiguration.DB_ARG, DummyDb.class.getName());
        requiredParamsAsMap.put(ConsoleAndFileDriverConfiguration.WORKLOAD_ARG, LdbcSnbInteractiveWorkload.class.getName());
        requiredParamsAsMap.put(ConsoleAndFileDriverConfiguration.OPERATION_COUNT_ARG, Long.toString(1000));

        DriverConfiguration configurationFromTestResources =
                ConsoleAndFileDriverConfiguration.fromParamsMap(MapUtils.mergeMaps(configurationInTestResourcesAsMap, requiredParamsAsMap, true));

        DriverConfiguration configurationFromWorkloadsDirectory =
                ConsoleAndFileDriverConfiguration.fromParamsMap(MapUtils.mergeMaps(configurationInWorkloadsDirectoryAsMap, requiredParamsAsMap, true));

        assertThat(configurationFromTestResources, equalTo(configurationFromWorkloadsDirectory));
    }

    @Test
    public void shouldSerializeAndParsePeerIds() throws DriverConfigurationException {
        // Given
        Set<String> peerIds0 = Sets.newHashSet();
        Set<String> peerIds1 = Sets.newHashSet("1", "2");
        Set<String> peerIds2 = Sets.newHashSet("1", "2", "cows");

        // When
        boolean exceptionThrownWithEmptyPeerIds = false;
        try {
            ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds0);
        } catch (DriverConfigurationException e) {
            exceptionThrownWithEmptyPeerIds = true;
        }
        boolean exceptionThrownWithNonEmptyPeerIds = false;
        String peerIdsString1 = null;
        String peerIdsString2 = null;
        try {
            peerIdsString1 = ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds1);
            peerIdsString2 = ConsoleAndFileDriverConfiguration.serializePeerIdsToCommandline(peerIds2);
        } catch (DriverConfigurationException e) {
            exceptionThrownWithNonEmptyPeerIds = true;
        }

        // Then
        assertThat(exceptionThrownWithEmptyPeerIds, is(true));
        assertThat(exceptionThrownWithNonEmptyPeerIds, is(false));
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline(peerIdsString1), equalTo(peerIds1));
        assertThat(ConsoleAndFileDriverConfiguration.parsePeerIdsFromCommandline(peerIdsString2), equalTo(peerIds2));
    }
}