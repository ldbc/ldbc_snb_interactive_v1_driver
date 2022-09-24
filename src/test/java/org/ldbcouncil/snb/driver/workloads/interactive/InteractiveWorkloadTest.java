package org.ldbcouncil.snb.driver.workloads.interactive;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.Operation;
import org.ldbcouncil.snb.driver.Workload;
import org.ldbcouncil.snb.driver.control.ConsoleAndFileDriverConfiguration;
import org.ldbcouncil.snb.driver.control.DriverConfiguration;
import org.ldbcouncil.snb.driver.control.DriverConfigurationException;
import org.ldbcouncil.snb.driver.testutils.TestUtils;
import org.ldbcouncil.snb.driver.util.MapUtils;
import org.ldbcouncil.snb.driver.util.Tuple;
import org.ldbcouncil.snb.driver.util.Tuple2;
import org.ldbcouncil.snb.driver.workloads.ClassNameWorkloadFactory;
import org.ldbcouncil.snb.driver.workloads.WorkloadTest;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveDb;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import org.ldbcouncil.snb.driver.workloads.interactive.db.DummyLdbcSnbInteractiveOperationResultSets;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcNoResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.io.File;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class InteractiveWorkloadTest extends WorkloadTest
{
    private Workload workload() throws Exception
    {
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration
                .fromDefaults(
                        LdbcSnbInteractiveWorkloadConfiguration.class.getName(),
                        LdbcSnbInteractiveWorkload.class.getName(),
                        1
                ).applyArgs( LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                ).applyArg( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, "1.0"
                ).applyArg(
                        ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                        "false"
                ).applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                        "10"
                ).applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                        TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
                ).applyArg(
                        LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                        TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
                );
        Workload workload = new ClassNameWorkloadFactory( configuration.workloadClassName() ).createWorkload();
        workload.init( configuration );
        return workload;
    }

    private List<Tuple2<Operation,Object>> operationsAndResults() throws Exception
    {
        return Lists.newArrayList(
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read1(),
                        DummyLdbcSnbInteractiveOperationResultSets.read1Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read2(),
                        DummyLdbcSnbInteractiveOperationResultSets.read2Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read3a(),
                        DummyLdbcSnbInteractiveOperationResultSets.read3Results()
                ),
                Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.read3b(),
                    DummyLdbcSnbInteractiveOperationResultSets.read3Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read4(),
                        DummyLdbcSnbInteractiveOperationResultSets.read4Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read5(),
                        DummyLdbcSnbInteractiveOperationResultSets.read5Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read6(),
                        DummyLdbcSnbInteractiveOperationResultSets.read6Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read7(),
                        DummyLdbcSnbInteractiveOperationResultSets.read7Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read8(),
                        DummyLdbcSnbInteractiveOperationResultSets.read8Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read9(),
                        DummyLdbcSnbInteractiveOperationResultSets.read9Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read10(),
                        DummyLdbcSnbInteractiveOperationResultSets.read10Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read11(),
                        DummyLdbcSnbInteractiveOperationResultSets.read11Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read12(),
                        DummyLdbcSnbInteractiveOperationResultSets.read12Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read13a(),
                        DummyLdbcSnbInteractiveOperationResultSets.read13Results()
                ),
                Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.read13b(),
                    DummyLdbcSnbInteractiveOperationResultSets.read13Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read14a(),
                        DummyLdbcSnbInteractiveOperationResultSets.read14Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read14b(),
                        DummyLdbcSnbInteractiveOperationResultSets.read14Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short1(),
                        DummyLdbcSnbInteractiveOperationResultSets.short1Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short2(),
                        DummyLdbcSnbInteractiveOperationResultSets.short2Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short3(),
                        DummyLdbcSnbInteractiveOperationResultSets.short3Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short4(),
                        DummyLdbcSnbInteractiveOperationResultSets.short4Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short5(),
                        DummyLdbcSnbInteractiveOperationResultSets.short5Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short6(),
                        DummyLdbcSnbInteractiveOperationResultSets.short6Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short7(),
                        DummyLdbcSnbInteractiveOperationResultSets.short7Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.short7(),
                        DummyLdbcSnbInteractiveOperationResultSets.short7Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write1(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write2(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write3(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write4(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write5(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write6(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write7(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.write8(),
                        LdbcNoResult.INSTANCE
                ),
                Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete1(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete2(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete3(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete4(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete5(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete6(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete7(),
                    LdbcNoResult.INSTANCE
            ),
            Tuple.<Operation,Object>tuple2(
                    DummyLdbcSnbInteractiveOperationInstances.delete8(),
                    LdbcNoResult.INSTANCE
            )
        );
    }

    public static DriverConfiguration configurationWithLongReadsOnly() throws DriverConfigurationException, IOException
    {
        // LONG READS ONLY, NO SHORT READS AND NO WRITES
        return ConsoleAndFileDriverConfiguration
            .fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                1_000_000
            ).applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 0 )
            ).applyArgs(
                LdbcSnbInteractiveWorkloadConfiguration.withoutWrites(
                        LdbcSnbInteractiveWorkloadConfiguration.withoutShortReads(
                                LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                        )
                )
            ).applyArg(
            LdbcSnbInteractiveWorkloadConfiguration.SCALE_FACTOR, Long.toString(1)
            ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                "false"
            ).applyArg(
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                "0.0000001"
            ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArg( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArgs(
                MapUtils.loadPropertiesToMap(
                        TestUtils.getResource( "/snb/interactive/updateStream.properties" ) )
            );
    }

    public static DriverConfiguration configurationWithLongAndShortReads() throws DriverConfigurationException, IOException
    {
        // SHORT AND LONG READS, NO WRITES
        return ConsoleAndFileDriverConfiguration
            .fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
            1_000_000
            ).applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 100_000 )
            ).applyArgs(
                    LdbcSnbInteractiveWorkloadConfiguration.withoutWrites(
                            LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                    )
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                    "false"
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                    "0.000001"
            ).applyArg(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArg( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArgs(
                    MapUtils.loadPropertiesToMap(
                            TestUtils.getResource( "/snb/interactive/updateStream.properties" )
                    )
            );
    }

    public static DriverConfiguration configurationWithWritesOnly() throws DriverConfigurationException, IOException
    {
        // WRITES ONLY
        return ConsoleAndFileDriverConfiguration
            .fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
            50_000
            ).applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 1_000 )
            ).applyArgs(
                    LdbcSnbInteractiveWorkloadConfiguration.withoutLongReads(
                            LdbcSnbInteractiveWorkloadConfiguration.withoutShortReads(
                                    LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                            )
                    )
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                    "false"
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                    "0.00001"
            ).applyArg(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArg( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArgs(
                    MapUtils.loadPropertiesToMap(
                            TestUtils.getResource( "/snb/interactive/updateStream.properties" )
                    )
            );
    }

    public static DriverConfiguration configurationWithFullWorkloadWithoutWarmup() throws DriverConfigurationException, IOException
    {
        return ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
            1_000_000
            ).applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 0 )
            ).applyArgs(
                    LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                    "false"
            ).applyArg(
                    ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                    "0.0000001"
            ).applyArg(
                    LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArg( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                    TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
            ).applyArgs(
                    MapUtils.loadPropertiesToMap(
                            TestUtils.getResource( "/snb/interactive/updateStream.properties" )
                    )
            );
    }

    public static DriverConfiguration configurationWithFullWorkloadWithWarmup() throws DriverConfigurationException, IOException
    {
        // FULL WORKLOAD
        return ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                1_000_000
        ).applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 100_000 )
        ).applyArgs(
                LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                "true"
        ).applyArg(
                ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG,
                "0.001"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg( LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArgs(
                MapUtils.loadPropertiesToMap(
                        TestUtils.getResource( "/snb/interactive/updateStream.properties" )
                )
        );
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldGenerateManyOperationsInReasonableTimeForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        boolean workloadGeneratedOperationsBeforeTimeout = shouldGenerateManyOperationsInReasonableTime(configuration, temporaryFolder);
        assertTrue( workloadGeneratedOperationsBeforeTimeout );
    }

    @Test
    public void shouldBeAbleToSerializeAndMarshalAllOperationResultsForInteractiveWorkload() throws Exception
    {
        List<Tuple2<Operation,Object>> operationsAndResults = operationsAndResults();
        shouldBeAbleToSerializeAndMarshalAllOperationResults(operationsAndResults);
    }

    @Test
    public void shouldBeAbleToSerializeAndMarshalAllOperationsForInteractiveWorkload() throws Exception
    {
        Workload workload = workload();
        List<Tuple2<Operation,Object>> operationsAndResults = operationsAndResults();
        shouldBeAbleToSerializeAndMarshalAllOperations(workload, operationsAndResults);        
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactoriesForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        shouldBeRepeatableWhenTwoIdenticalWorkloadsAreUsedWithIdenticalGeneratorFactories(configuration, temporaryFolder);
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldLoadFromConfigFileForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        shouldLoadFromConfigFile(configuration, temporaryFolder);
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperationsForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        shouldAssignMonotonicallyIncreasingScheduledStartTimesToOperations(configuration, temporaryFolder);
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldRunWorkloadWithForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        shouldRunWorkload(configuration, temporaryFolder);
    }

    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPassForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        shouldCreateValidationParametersThenUseThemToPerformDatabaseValidationThenPass(configuration, temporaryFolder);
    }

    @Disabled
    @ParameterizedTest
    @MethodSource("configurations")
    public void shouldPassWorkloadValidationForInteractiveWorkload(DriverConfiguration configuration, @TempDir File temporaryFolder) throws Exception
    {
        shouldPassWorkloadValidation(configuration, temporaryFolder);
    }

    private static Stream<Arguments> configurations() throws Exception{
        return Stream.of(
                Arguments.of(configurationWithLongReadsOnly()),
                Arguments.of(configurationWithLongAndShortReads()),
                Arguments.of(configurationWithWritesOnly()),
                Arguments.of(configurationWithFullWorkloadWithoutWarmup()),
                Arguments.of(configurationWithFullWorkloadWithWarmup())
        );
    }

    @Test
    public void shouldHaveNonNegativeTypesForAllOperationsForInteractiveWorkload() throws Exception
    {
        shouldHaveNonNegativeTypesForAllOperations(workload(), operationsAndResults());
    }

    @Test
    public void shouldHaveOneToOneMappingBetweenOperationClassesAndOperationTypesForInteractiveWorkload() throws Exception
    {
        shouldHaveOneToOneMappingBetweenOperationClassesAndOperationTypes(workload());
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndAllUpdatesEnabled() throws Exception
    {
        // Given
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                1
        ).applyArgs(
                LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY,
                "10"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_FREQUENCY_KEY,
                "20"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_FREQUENCY_KEY,
                "30"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_FREQUENCY_KEY,
                "30"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_FREQUENCY_KEY,
                "40"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_FREQUENCY_KEY,
                "50"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_FREQUENCY_KEY,
                "60"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_FREQUENCY_KEY,
                "70"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_FREQUENCY_KEY,
                "80"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_FREQUENCY_KEY,
                "90"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_FREQUENCY_KEY,
                "100"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_FREQUENCY_KEY,
                "200"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_FREQUENCY_KEY,
                "300"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_FREQUENCY_KEY,
                "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_FREQUENCY_KEY,
                    "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_FREQUENCY_KEY,
                "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_FREQUENCY_KEY,
                "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                "10"
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        );

        // When
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        // Then

        Map<String,String> configurationAsMap = configuration.asMap();
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY ),
                equalTo( "100" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY ),
                equalTo( "200" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_INTERLEAVE_KEY ),
                equalTo( "300" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_INTERLEAVE_KEY ),
                equalTo( "300" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ),
                equalTo( "400" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ),
                equalTo( "500" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ),
                equalTo( "600" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ),
                equalTo( "700" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ),
                equalTo( "800" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ),
                equalTo( "900" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ),
                equalTo( "1000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ),
                equalTo( "2000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ),
                equalTo( "3000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_INTERLEAVE_KEY ),
                equalTo( "4000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_INTERLEAVE_KEY ),
                equalTo( "4000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_INTERLEAVE_KEY ),
                equalTo( "5000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_INTERLEAVE_KEY ),
                equalTo( "5000" ) );
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndOnlyOneUpdateEnabled()
            throws Exception
    {
        // Given
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                1
        ).applyArgs(
                LdbcSnbInteractiveWorkloadConfiguration.withoutWrites(
                        LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                )
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.WRITE_OPERATION_1_ENABLE_KEY,
                "true"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY,
                "10"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_FREQUENCY_KEY,
                "20"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_FREQUENCY_KEY,
                "30"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_FREQUENCY_KEY,
                "30"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_FREQUENCY_KEY,
                "40"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_FREQUENCY_KEY,
                "50"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_FREQUENCY_KEY,
                "60"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_FREQUENCY_KEY,
                "70"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_FREQUENCY_KEY,
                "80"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_FREQUENCY_KEY,
                "90"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_FREQUENCY_KEY,
                "100"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_FREQUENCY_KEY,
                "200"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_FREQUENCY_KEY,
                "300"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_FREQUENCY_KEY,
                "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_FREQUENCY_KEY,
                "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_FREQUENCY_KEY,
                "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_FREQUENCY_KEY,
                "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                "10"
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        );

        // When
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        // Then

        Map<String,String> configurationAsMap = configuration.asMap();
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY ),
                equalTo( "100" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY ),
                equalTo( "200" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_INTERLEAVE_KEY ),
                equalTo( "300" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_INTERLEAVE_KEY ),
                equalTo( "300" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ),
                equalTo( "400" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ),
                equalTo( "500" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ),
                equalTo( "600" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ),
                equalTo( "700" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ),
                equalTo( "800" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ),
                equalTo( "900" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ),
                equalTo( "1000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ),
                equalTo( "2000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ),
                equalTo( "3000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_INTERLEAVE_KEY ),
                equalTo( "4000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_INTERLEAVE_KEY ),
                equalTo( "4000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_INTERLEAVE_KEY ),
                equalTo( "5000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_INTERLEAVE_KEY ),
                equalTo( "5000" ) );
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenAllFrequenciesProvidedAndAllUpdatesDisabled() throws Exception
    {
        // Given
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                1
        ).applyArgs(
                LdbcSnbInteractiveWorkloadConfiguration.withoutWrites(
                        LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                )
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY,
                "10"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_FREQUENCY_KEY,
                "20"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_FREQUENCY_KEY,
                "30"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_FREQUENCY_KEY,
                "30"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_FREQUENCY_KEY,
                "40"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_FREQUENCY_KEY,
                "50"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_FREQUENCY_KEY,
                "60"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_FREQUENCY_KEY,
                "70"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_FREQUENCY_KEY,
                "80"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_FREQUENCY_KEY,
                "90"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_FREQUENCY_KEY,
                "100"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_FREQUENCY_KEY,
                "200"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_FREQUENCY_KEY,
                "300"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_FREQUENCY_KEY,
                "400"
        ).applyArg(
            LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_FREQUENCY_KEY,
            "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_FREQUENCY_KEY,
                "500"
        ).applyArg(
            LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_FREQUENCY_KEY,
            "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                "10"
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        );

        // When
        Workload workload = new LdbcSnbInteractiveWorkload();
        workload.init( configuration );

        // Then

        Map<String,String> configurationAsMap = configuration.asMap();
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY ),
                equalTo( "100" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY ),
                equalTo( "200" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3a_INTERLEAVE_KEY ),
                equalTo( "300" ) );
        assertThat(
            configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3b_INTERLEAVE_KEY ),
            equalTo( "300" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY ),
                equalTo( "400" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY ),
                equalTo( "500" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY ),
                equalTo( "600" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY ),
                equalTo( "700" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY ),
                equalTo( "800" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY ),
                equalTo( "900" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY ),
                equalTo( "1000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY ),
                equalTo( "2000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY ),
                equalTo( "3000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13a_INTERLEAVE_KEY ),
                equalTo( "4000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13b_INTERLEAVE_KEY ),
                equalTo( "4000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14a_INTERLEAVE_KEY ),
                equalTo( "5000" ) );
        assertThat(
                configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14b_INTERLEAVE_KEY ),
                equalTo( "5000" ) );
    }
}
