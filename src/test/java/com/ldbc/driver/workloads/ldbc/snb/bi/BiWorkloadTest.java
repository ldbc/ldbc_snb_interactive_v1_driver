package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.Workload;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.control.ConsoleAndFileDriverConfiguration;
import com.ldbc.driver.control.DriverConfiguration;
import com.ldbc.driver.control.DriverConfigurationException;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.MapUtils;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import com.ldbc.driver.util.TypeChangeFun;
import com.ldbc.driver.workloads.ClassNameWorkloadFactory;
import com.ldbc.driver.workloads.OperationMixBuilder;
import com.ldbc.driver.workloads.WorkloadTest;
import com.ldbc.driver.workloads.ldbc.snb.bi.db.DummyLdbcSnbBiDb;
import com.ldbc.driver.workloads.ldbc.snb.bi.db.DummyLdbcSnbBiOperationInstances;
import com.ldbc.driver.workloads.ldbc.snb.bi.db.DummyLdbcSnbBiOperationResultInstances;
import com.ldbc.driver.workloads.ldbc.snb.bi.db.DummyLdbcSnbBiOperationResultSets;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class BiWorkloadTest extends WorkloadTest
{
    @Override
    public Workload workload() throws Exception
    {
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbBiDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
                1
        ).applyArgs(
                LdbcSnbBiWorkloadConfiguration.defaultConfigSF1()
        ).applyArg(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
        );
        Workload workload = new ClassNameWorkloadFactory( configuration.workloadClassName() ).createWorkload();
        workload.init( configuration );
        return workload;
    }

    @Override
    public List<Tuple2<Operation,Object>> operationsAndResults()
    {
        return Lists.newArrayList(
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read1(),
                        DummyLdbcSnbBiOperationResultSets.read1Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read2(),
                        DummyLdbcSnbBiOperationResultSets.read2Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read3(),
                        DummyLdbcSnbBiOperationResultSets.read3Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read4(),
                        DummyLdbcSnbBiOperationResultSets.read4Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read5(),
                        DummyLdbcSnbBiOperationResultSets.read5Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read6(),
                        DummyLdbcSnbBiOperationResultSets.read6Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read7(),
                        DummyLdbcSnbBiOperationResultSets.read7Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read8(),
                        DummyLdbcSnbBiOperationResultSets.read8Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read9(),
                        DummyLdbcSnbBiOperationResultSets.read9Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read10(),
                        DummyLdbcSnbBiOperationResultSets.read10Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read11(),
                        DummyLdbcSnbBiOperationResultSets.read11Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read12(),
                        DummyLdbcSnbBiOperationResultSets.read12Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read13(),
                        DummyLdbcSnbBiOperationResultSets.read13Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read14(),
                        DummyLdbcSnbBiOperationResultSets.read14Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read15(),
                        DummyLdbcSnbBiOperationResultSets.read15Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read16(),
                        DummyLdbcSnbBiOperationResultSets.read16Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read17(),
                        DummyLdbcSnbBiOperationResultInstances.read17Result()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read18(),
                        DummyLdbcSnbBiOperationResultSets.read18Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read19(),
                        DummyLdbcSnbBiOperationResultSets.read19Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read20(),
                        DummyLdbcSnbBiOperationResultSets.read20Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read21(),
                        DummyLdbcSnbBiOperationResultSets.read21Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read22(),
                        DummyLdbcSnbBiOperationResultSets.read22Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read23(),
                        DummyLdbcSnbBiOperationResultSets.read23Results()
                ),
                Tuple.<Operation,Object>tuple2(
                        DummyLdbcSnbBiOperationInstances.read24(),
                        DummyLdbcSnbBiOperationResultSets.read24Results()
                )
        );
    }

    @Override
    public List<DriverConfiguration> configurations() throws Exception
    {
        return Lists.newArrayList(
                ConsoleAndFileDriverConfiguration
                        .fromDefaults(
                                DummyLdbcSnbBiDb.class.getName(),
                                LdbcSnbBiWorkload.class.getName(),
                                1_000_000
                        )
                        .applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 0 ) )
                        .applyArgs( LdbcSnbBiWorkloadConfiguration.defaultConfigSF1() )
                        .applyArg(
                                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                                TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
                        )
                        .applyArg( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, "0.2" )
                        .applyArg(
                                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                                "false"
                        ),
                ConsoleAndFileDriverConfiguration
                        .fromDefaults(
                                DummyLdbcSnbBiDb.class.getName(),
                                LdbcSnbBiWorkload.class.getName(),
                                1_000_000
                        )
                        .applyArg( ConsoleAndFileDriverConfiguration.WARMUP_COUNT_ARG, Long.toString( 1_000_000 ) )
                        .applyArgs( LdbcSnbBiWorkloadConfiguration.defaultConfigSF1() )
                        .applyArg(
                                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                                TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
                        )
                        .applyArg( ConsoleAndFileDriverConfiguration.TIME_COMPRESSION_RATIO_ARG, "0.2" )
                        .applyArg(
                                ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG,
                                "true"
                        )
        );
    }

    @Override
    public List<Tuple2<DriverConfiguration,Histogram<Class,Double>>> configurationsWithExpectedQueryMix()
            throws Exception
    {
        Histogram<Class,Double> expectedQueryMixHistogram = new Histogram<>( 0d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery1PostingSummary.class ), 1d );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery2TopTags.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery3TagEvolution.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery4PopularCountryTopics.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery5TopCountryPosters.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery6ActivePosters.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery7AuthoritativeUsers.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery8RelatedTopics.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery9RelatedForums.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery10TagPerson.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery11UnrelatedReplies.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery12TrendingPosts.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery13PopularMonthlyTags.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery14TopThreadInitiators.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery15SocialNormals.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery16ExpertsInSocialCircle.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery17FriendshipTriangles.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery18PersonPostCounts.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery19StrangerInteraction.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery20HighLevelTopics.class ), 1d );
        expectedQueryMixHistogram.addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery21Zombies.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery22InternationalDialog.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery23HolidayDestinations.class ), 1d );
        expectedQueryMixHistogram
                .addBucket( Bucket.DiscreteBucket.<Class>create( LdbcSnbBiQuery24MessagesByTopic.class ), 1d );

        return Lists.newArrayList(
                Tuple.tuple2(
                        ConsoleAndFileDriverConfiguration
                                .fromDefaults(
                                        DummyLdbcSnbBiDb.class.getName(),
                                        LdbcSnbBiWorkload.class.getName(),
                                        1_000_000
                                )
                                .applyArgs( LdbcSnbBiWorkloadConfiguration.defaultConfigSF1() )
                                .applyArg( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true" )
                                .applyArg(
                                        LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                                        TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
                                ),
                        expectedQueryMixHistogram
                )
        );
    }

    @Test
    public void shouldConvertFrequenciesToInterleavesWhenFrequenciesProvidedAndSomeInterleavesNotProvided()
            throws Exception
    {
        // Given
        long updateInterleave = 10;
        OperationMixBuilder.OperationMix operationMix = OperationMixBuilder.fromFrequencies( updateInterleave )
                .addOperationFrequency( LdbcSnbBiQuery1PostingSummary.TYPE, 10 )
                .addOperationFrequency( LdbcSnbBiQuery2TopTags.TYPE, 20 )
                .addOperationFrequency( LdbcSnbBiQuery3TagEvolution.TYPE, 30 )
                .addOperationFrequency( LdbcSnbBiQuery4PopularCountryTopics.TYPE, 40 )
                .addOperationFrequency( LdbcSnbBiQuery5TopCountryPosters.TYPE, 50 )
                .addOperationFrequency( LdbcSnbBiQuery6ActivePosters.TYPE, 60 )
                .addOperationFrequency( LdbcSnbBiQuery7AuthoritativeUsers.TYPE, 70 )
                .addOperationFrequency( LdbcSnbBiQuery8RelatedTopics.TYPE, 80 )
                .addOperationFrequency( LdbcSnbBiQuery9RelatedForums.TYPE, 90 )
                .addOperationFrequency( LdbcSnbBiQuery10TagPerson.TYPE, 100 )
                .addOperationFrequency( LdbcSnbBiQuery11UnrelatedReplies.TYPE, 200 )
                .addOperationFrequency( LdbcSnbBiQuery12TrendingPosts.TYPE, 300 )
                .addOperationFrequency( LdbcSnbBiQuery13PopularMonthlyTags.TYPE, 400 )
                .addOperationFrequency( LdbcSnbBiQuery14TopThreadInitiators.TYPE, 500 )
                .addOperationFrequency( LdbcSnbBiQuery15SocialNormals.TYPE, 600 )
                .addOperationFrequency( LdbcSnbBiQuery16ExpertsInSocialCircle.TYPE, 700 )
                .addOperationFrequency( LdbcSnbBiQuery17FriendshipTriangles.TYPE, 800 )
                .addOperationFrequency( LdbcSnbBiQuery18PersonPostCounts.TYPE, 900 )
                .addOperationFrequency( LdbcSnbBiQuery19StrangerInteraction.TYPE, 1000 )
                .addOperationFrequency( LdbcSnbBiQuery20HighLevelTopics.TYPE, 10 )
                .addOperationFrequency( LdbcSnbBiQuery21Zombies.TYPE, 20 )
                .addOperationFrequency( LdbcSnbBiQuery22InternationalDialog.TYPE, 30 )
                .addOperationFrequency( LdbcSnbBiQuery23HolidayDestinations.TYPE, 40 )
                .addOperationFrequency( LdbcSnbBiQuery24MessagesByTopic.TYPE, 50 )
                .build();
        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbBiDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
                1
        ).applyArgs(
                LdbcSnbBiWorkloadConfiguration.defaultConfigSF1()
        ).applyArg(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
        ).applyArg( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true"
        ).applyArgs(
                MapUtils.UNSAFE_changeTypes(
                        operationMix.interleaves(),
                        TypeChangeFun.mapped( LdbcSnbBiWorkloadConfiguration.OPERATION_TYPE_TO_INTERLEAVE_KEY_MAPPING ),
                        TypeChangeFun.TO_STRING
                )
        );

        // When
        try ( Workload workload = new LdbcSnbBiWorkload() )
        {
            workload.init( configuration );

            // Then

            Map<String,String> configurationAsMap = configuration.asMap();
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_1_INTERLEAVE_KEY ),
                    equalTo( "100" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_2_INTERLEAVE_KEY ),
                    equalTo( "200" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_3_INTERLEAVE_KEY ),
                    equalTo( "300" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_4_INTERLEAVE_KEY ),
                    equalTo( "400" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_5_INTERLEAVE_KEY ),
                    equalTo( "500" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_6_INTERLEAVE_KEY ),
                    equalTo( "600" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_7_INTERLEAVE_KEY ),
                    equalTo( "700" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_8_INTERLEAVE_KEY ),
                    equalTo( "800" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_9_INTERLEAVE_KEY ),
                    equalTo( "900" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_10_INTERLEAVE_KEY ),
                    equalTo( "1000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_11_INTERLEAVE_KEY ),
                    equalTo( "2000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_12_INTERLEAVE_KEY ),
                    equalTo( "3000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_13_INTERLEAVE_KEY ),
                    equalTo( "4000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_14_INTERLEAVE_KEY ),
                    equalTo( "5000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_15_INTERLEAVE_KEY ),
                    equalTo( "6000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_16_INTERLEAVE_KEY ),
                    equalTo( "7000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_17_INTERLEAVE_KEY ),
                    equalTo( "8000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_18_INTERLEAVE_KEY ),
                    equalTo( "9000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_19_INTERLEAVE_KEY ),
                    equalTo( "10000" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_20_INTERLEAVE_KEY ),
                    equalTo( "100" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_21_INTERLEAVE_KEY ),
                    equalTo( "200" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_22_INTERLEAVE_KEY ),
                    equalTo( "300" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_23_INTERLEAVE_KEY ),
                    equalTo( "400" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_24_INTERLEAVE_KEY ),
                    equalTo( "500" ) );
        }
    }

    @Test
    public void shouldThrowExceptionWhenSomeFrequenciesNotProvidedAndSomeInterleavesNoProvided()
            throws WorkloadException, DriverConfigurationException, IOException
    {
        // Given
        Map<String,Long> operationMixMap = new HashMap<>();
//        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_1_INTERLEAVE_KEY ,1l);
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_2_INTERLEAVE_KEY, 2l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_3_INTERLEAVE_KEY, 3l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_4_INTERLEAVE_KEY, 4l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_5_INTERLEAVE_KEY, 5l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_6_INTERLEAVE_KEY, 6l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_7_INTERLEAVE_KEY, 7l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_8_INTERLEAVE_KEY, 8l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_9_INTERLEAVE_KEY, 9l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_10_INTERLEAVE_KEY, 10l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_11_INTERLEAVE_KEY, 11l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_12_INTERLEAVE_KEY, 12l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_13_INTERLEAVE_KEY, 13l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_14_INTERLEAVE_KEY, 14l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_15_INTERLEAVE_KEY, 15l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_16_INTERLEAVE_KEY, 16l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_17_INTERLEAVE_KEY, 17l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_18_INTERLEAVE_KEY, 18l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_19_INTERLEAVE_KEY, 19l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_20_INTERLEAVE_KEY, 20l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_21_INTERLEAVE_KEY, 21l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_22_INTERLEAVE_KEY, 22l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_23_INTERLEAVE_KEY, 23l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_24_INTERLEAVE_KEY, 24l );
//        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_1_FREQUENCY_KEY ,1l);
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_2_FREQUENCY_KEY, 2l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_3_FREQUENCY_KEY, 3l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_4_FREQUENCY_KEY, 4l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_5_FREQUENCY_KEY, 5l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_6_FREQUENCY_KEY, 6l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_7_FREQUENCY_KEY, 7l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_8_FREQUENCY_KEY, 8l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_9_FREQUENCY_KEY, 9l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_10_FREQUENCY_KEY, 10l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_11_FREQUENCY_KEY, 11l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_12_FREQUENCY_KEY, 12l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_13_FREQUENCY_KEY, 13l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_14_FREQUENCY_KEY, 14l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_15_FREQUENCY_KEY, 15l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_16_FREQUENCY_KEY, 16l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_17_FREQUENCY_KEY, 17l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_18_FREQUENCY_KEY, 18l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_19_FREQUENCY_KEY, 19l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_20_FREQUENCY_KEY, 20l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_21_FREQUENCY_KEY, 21l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_22_FREQUENCY_KEY, 22l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_23_FREQUENCY_KEY, 23l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_24_FREQUENCY_KEY, 24l );

        Map<String,String> defaultSnbBiParams = LdbcSnbBiWorkloadConfiguration.defaultConfigSF1();
        defaultSnbBiParams.remove( LdbcSnbBiWorkloadConfiguration.OPERATION_1_INTERLEAVE_KEY );
        defaultSnbBiParams.remove( LdbcSnbBiWorkloadConfiguration.OPERATION_1_FREQUENCY_KEY );
        defaultSnbBiParams.put(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbBiDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
                1 )
                .applyArg( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true" )
                .applyArgs( defaultSnbBiParams )
                .applyArgs(
                        MapUtils.UNSAFE_changeTypes(
                                operationMixMap,
                                TypeChangeFun.IDENTITY,
                                TypeChangeFun.TO_STRING
                        ) );

        // When
        boolean exceptionThrown = false;
        try ( Workload workload = new LdbcSnbBiWorkload() )
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

    @Test
    public void shouldUseInterleavesWhenAllInterleavesProvided()
            throws WorkloadException, DriverConfigurationException, IOException
    {
        // Given
        Map<String,Long> operationMixMap = new HashMap<>();
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_1_INTERLEAVE_KEY, 10l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_2_INTERLEAVE_KEY, 20l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_3_INTERLEAVE_KEY, 30l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_4_INTERLEAVE_KEY, 40l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_5_INTERLEAVE_KEY, 50l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_6_INTERLEAVE_KEY, 60l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_7_INTERLEAVE_KEY, 70l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_8_INTERLEAVE_KEY, 80l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_9_INTERLEAVE_KEY, 90l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_10_INTERLEAVE_KEY, 100l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_11_INTERLEAVE_KEY, 110l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_12_INTERLEAVE_KEY, 120l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_13_INTERLEAVE_KEY, 130l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_14_INTERLEAVE_KEY, 140l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_15_INTERLEAVE_KEY, 150l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_16_INTERLEAVE_KEY, 160l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_17_INTERLEAVE_KEY, 170l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_18_INTERLEAVE_KEY, 180l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_19_INTERLEAVE_KEY, 190l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_20_INTERLEAVE_KEY, 200l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_21_INTERLEAVE_KEY, 210l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_22_INTERLEAVE_KEY, 220l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_23_INTERLEAVE_KEY, 230l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_24_INTERLEAVE_KEY, 240l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_1_FREQUENCY_KEY ,1l);
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_2_FREQUENCY_KEY, 2l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_3_FREQUENCY_KEY, 3l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_4_FREQUENCY_KEY, 4l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_5_FREQUENCY_KEY, 5l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_6_FREQUENCY_KEY, 6l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_7_FREQUENCY_KEY, 7l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_8_FREQUENCY_KEY, 8l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_9_FREQUENCY_KEY, 9l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_10_FREQUENCY_KEY, 10l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_11_FREQUENCY_KEY, 11l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_12_FREQUENCY_KEY, 12l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_13_FREQUENCY_KEY, 13l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_14_FREQUENCY_KEY, 14l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_15_FREQUENCY_KEY, 15l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_16_FREQUENCY_KEY, 16l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_17_FREQUENCY_KEY, 17l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_18_FREQUENCY_KEY, 18l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_19_FREQUENCY_KEY, 19l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_20_FREQUENCY_KEY, 20l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_21_FREQUENCY_KEY, 21l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_22_FREQUENCY_KEY, 22l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_23_FREQUENCY_KEY, 23l );
        operationMixMap.put( LdbcSnbBiWorkloadConfiguration.OPERATION_24_FREQUENCY_KEY, 24l );

        Map<String,String> defaultSnbBiParams = LdbcSnbBiWorkloadConfiguration.defaultConfigSF1();
        defaultSnbBiParams.remove( LdbcSnbBiWorkloadConfiguration.OPERATION_1_FREQUENCY_KEY );
        defaultSnbBiParams.put(
                LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                TestUtils.getResource( "/snb/bi/" ).getAbsolutePath()
        );

        DriverConfiguration configuration = ConsoleAndFileDriverConfiguration.fromDefaults(
                DummyLdbcSnbBiDb.class.getName(),
                LdbcSnbBiWorkload.class.getName(),
                1 )
                .applyArg( ConsoleAndFileDriverConfiguration.IGNORE_SCHEDULED_START_TIMES_ARG, "true" )
                .applyArgs( defaultSnbBiParams )
                .applyArg(
                        LdbcSnbBiWorkloadConfiguration.PARAMETERS_DIRECTORY,
                        TestUtils.getResource( "/snb/bi/" ).getAbsolutePath() )
                .applyArgs(
                        MapUtils.UNSAFE_changeTypes(
                                operationMixMap,
                                TypeChangeFun.IDENTITY,
                                TypeChangeFun.TO_STRING
                        ) );

        // When
        try ( Workload workload = new LdbcSnbBiWorkload() )
        {
            workload.init( configuration );

            // Then
            Map<String,String> configurationAsMap = configuration.asMap();
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_1_INTERLEAVE_KEY ),
                    equalTo( "10" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_2_INTERLEAVE_KEY ),
                    equalTo( "20" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_3_INTERLEAVE_KEY ),
                    equalTo( "30" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_4_INTERLEAVE_KEY ),
                    equalTo( "40" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_5_INTERLEAVE_KEY ),
                    equalTo( "50" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_6_INTERLEAVE_KEY ),
                    equalTo( "60" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_7_INTERLEAVE_KEY ),
                    equalTo( "70" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_8_INTERLEAVE_KEY ),
                    equalTo( "80" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_9_INTERLEAVE_KEY ),
                    equalTo( "90" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_10_INTERLEAVE_KEY ),
                    equalTo( "100" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_11_INTERLEAVE_KEY ),
                    equalTo( "110" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_12_INTERLEAVE_KEY ),
                    equalTo( "120" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_13_INTERLEAVE_KEY ),
                    equalTo( "130" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_14_INTERLEAVE_KEY ),
                    equalTo( "140" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_15_INTERLEAVE_KEY ),
                    equalTo( "150" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_16_INTERLEAVE_KEY ),
                    equalTo( "160" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_17_INTERLEAVE_KEY ),
                    equalTo( "170" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_18_INTERLEAVE_KEY ),
                    equalTo( "180" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_19_INTERLEAVE_KEY ),
                    equalTo( "190" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_20_INTERLEAVE_KEY ),
                    equalTo( "200" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_21_INTERLEAVE_KEY ),
                    equalTo( "210" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_22_INTERLEAVE_KEY ),
                    equalTo( "220" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_23_INTERLEAVE_KEY ),
                    equalTo( "230" ) );
            assertThat(
                    configurationAsMap.get( LdbcSnbBiWorkloadConfiguration.OPERATION_24_INTERLEAVE_KEY ),
                    equalTo( "240" ) );
        }
    }
}
