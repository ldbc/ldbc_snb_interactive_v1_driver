package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.util.TypeChangeFun;
import com.ldbc.driver.workloads.ClassNameWorkloadFactory;
import com.ldbc.driver.workloads.WorkloadTest;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveDb;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationInstances;
import com.ldbc.driver.workloads.ldbc.snb.interactive.db.DummyLdbcSnbInteractiveOperationResultSets;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class InteractiveWorkloadTest extends WorkloadTest
{
    @Override
    public Workload workload() throws Exception
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

    @Override
    public List<Tuple2<Operation,Object>> operationsAndResults() throws Exception
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
                        DummyLdbcSnbInteractiveOperationInstances.read3(),
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
                        DummyLdbcSnbInteractiveOperationInstances.read13(),
                        DummyLdbcSnbInteractiveOperationResultSets.read13Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbInteractiveOperationInstances.read14(),
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
                )
        );
    }

    @Override
    public List<DriverConfiguration> configurations() throws Exception
    {
        return Lists.newArrayList(
                // LONG READS ONLY, NO SHORT READS AND NO WRITES
                ConsoleAndFileDriverConfiguration.fromDefaults(
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
                ),
                // SHORT AND LONG READS, NO WRITES
                ConsoleAndFileDriverConfiguration.fromDefaults(
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
                ),
                // WRITES ONLY
                ConsoleAndFileDriverConfiguration.fromDefaults(
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
                ),
                // FULL WORKLOAD
                ConsoleAndFileDriverConfiguration.fromDefaults(
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
                ),
                // FULL WORKLOAD
                ConsoleAndFileDriverConfiguration.fromDefaults(
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
                )
        );
    }

    @Override
    public List<Tuple2<DriverConfiguration,Histogram<Class,Double>>> configurationsWithExpectedQueryMix()
            throws Exception
    {
        Histogram<Class,Double> expectedQueryMixHistogram = new Histogram<>( 0d );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery1.class ), 1d / 100 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery2.class ), 1d / 200 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery3.class ), 1d / 400 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery4.class ), 1d / 800 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery5.class ), 1d / 1600 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery6.class ), 1d / 1600 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery7.class ), 1d / 800 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery8.class ), 1d / 800 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery9.class ), 1d / 400 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery10.class ), 1d / 200 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery11.class ), 1d / 200 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery12.class ), 1d / 200 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery13.class ), 1d / 100 );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.create( (Class) LdbcQuery14.class ), 1d / 100 );

        Map<String,String> defaultSnbInteractiveParams =
                LdbcSnbInteractiveWorkloadConfiguration.withoutShortReads(
                        LdbcSnbInteractiveWorkloadConfiguration.withoutWrites(
                                LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1()
                        )
                );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_FREQUENCY_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_FREQUENCY_KEY );
        return Lists.newArrayList(
                Tuple.tuple2(
                        ConsoleAndFileDriverConfiguration.fromDefaults(
                                DummyLdbcSnbInteractiveDb.class.getName(),
                                LdbcSnbInteractiveWorkload.class.getName(),
                                1_000_000
                        ).applyArgs(
                                defaultSnbInteractiveParams
                        ).applyArg(
                                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                                "true"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY, "100"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY, "200"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY, "400"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY, "800"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY, "1600"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY, "1600"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY, "800"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY, "800"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY, "400"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY, "200"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY, "200"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY, "200"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY, "100"
                        ).applyArg(
                                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY, "100"
                        ),
                        expectedQueryMixHistogram
                )
        );
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
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_FREQUENCY_KEY,
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
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_FREQUENCY_KEY,
                "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_FREQUENCY_KEY,
                "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                "10"
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        );

        // When
        try ( Workload workload = new LdbcSnbInteractiveWorkload() )
        {
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
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY ),
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
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY ),
                    equalTo( "4000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY ),
                    equalTo( "5000" ) );
        }
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
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_FREQUENCY_KEY,
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
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_FREQUENCY_KEY,
                "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_FREQUENCY_KEY,
                "500"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                "10"
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        );

        // When
        try ( Workload workload = new LdbcSnbInteractiveWorkload() )
        {
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
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY ),
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
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY ),
                    equalTo( "4000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY ),
                    equalTo( "5000" ) );
        }
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
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_FREQUENCY_KEY,
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
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_FREQUENCY_KEY,
                "400"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_FREQUENCY_KEY,
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
        try ( Workload workload = new LdbcSnbInteractiveWorkload() )
        {
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
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY ),
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
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY ),
                    equalTo( "4000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY ),
                    equalTo( "5000" ) );
        }
    }

    @Test
    public void shouldThrowExceptionWhenSomeFrequenciesNotProvidedAndSomeInterleavesNoProvided() throws Exception
    {
        // Given
        Map<String,Long> operationMixMap = new HashMap<>();
//        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY, 1l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_INTERLEAVE_KEY, 2l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_INTERLEAVE_KEY, 3l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_INTERLEAVE_KEY, 4l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_INTERLEAVE_KEY, 5l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_INTERLEAVE_KEY, 6l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_INTERLEAVE_KEY, 7l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_INTERLEAVE_KEY, 8l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_INTERLEAVE_KEY, 9l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_INTERLEAVE_KEY, 10l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_INTERLEAVE_KEY, 11l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_INTERLEAVE_KEY, 12l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_INTERLEAVE_KEY, 13l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_INTERLEAVE_KEY, 14l );
//        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY, 1l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_2_FREQUENCY_KEY, 2l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_3_FREQUENCY_KEY, 3l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_4_FREQUENCY_KEY, 4l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_5_FREQUENCY_KEY, 5l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_6_FREQUENCY_KEY, 6l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_7_FREQUENCY_KEY, 7l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_8_FREQUENCY_KEY, 8l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_9_FREQUENCY_KEY, 9l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_10_FREQUENCY_KEY, 10l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_11_FREQUENCY_KEY, 11l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_12_FREQUENCY_KEY, 12l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_13_FREQUENCY_KEY, 13l );
        operationMixMap.put( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_14_FREQUENCY_KEY, 14l );

        Map<String,String> defaultSnbInteractiveParams = LdbcSnbInteractiveWorkloadConfiguration.defaultConfigSF1();
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_INTERLEAVE_KEY );
        defaultSnbInteractiveParams.remove( LdbcSnbInteractiveWorkloadConfiguration.READ_OPERATION_1_FREQUENCY_KEY );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbInteractiveDb.class.getName(),
                LdbcSnbInteractiveWorkload.class.getName(),
                1
        ).applyArg(
                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        ).applyArgs(
                defaultSnbInteractiveParams
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATE_INTERLEAVE,
                "10"
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArg(
                LdbcSnbInteractiveWorkloadConfiguration.UPDATES_DIRECTORY,
                TestUtils.getResource( "/snb/interactive/" ).getAbsolutePath()
        ).applyArgs(
                MapUtils.UNSAFE_changeTypes(
                        operationMixMap,
                        TypeChangeFun.IDENTITY,
                        TypeChangeFun.TO_STRING
                )
        );

        // When
        boolean exceptionThrown = false;
        try ( Workload workload = new LdbcSnbInteractiveWorkload() )
        {
            workload.init( configuration );
        }
        catch ( WorkloadException e )
        {
            System.out.println( e.getMessage() );
            exceptionThrown = true;
        }

        // Then
        // either interleaves or frequencies need to be provided
        assertTrue( exceptionThrown );
    }
}
