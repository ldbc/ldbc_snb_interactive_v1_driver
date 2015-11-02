package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPersonResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedReplies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPosts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiatorsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormals;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormalsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTrianglesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCounts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCountsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteractionResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummaryResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Zombies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21ZombiesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialogResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinationsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopic;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopicResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolution;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolutionResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPostersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePostersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsers;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForums;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForumsResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class DummyLdbcSnbBiDb extends Db
{
    private static class DummyDbConnectionState extends DbConnectionState
    {
        @Override
        public void close() throws IOException
        {
        }
    }

    public enum SleepType
    {
        THREAD_SLEEP,
        SPIN
    }

    public static final String SLEEP_DURATION_NANO_ARG = "ldbc.snb.bi.db.sleep_duration_nano";
    public static final String SLEEP_TYPE_ARG = "ldbc.snb.bi.db.sleep_type";

    private static long sleepDurationAsNano;
    private SleepType sleepType;

    private interface SleepFun
    {
        void sleep( long sleepNs );
    }

    private static SleepFun sleepFun;

    @Override
    protected void onInit( Map<String,String> properties, LoggingService loggingService ) throws DbException
    {
        String sleepDurationAsNanoAsString = properties.get( SLEEP_DURATION_NANO_ARG );
        if ( null == sleepDurationAsNanoAsString )
        {
            sleepDurationAsNano = 0l;
        }
        else
        {
            try
            {
                sleepDurationAsNano = Long.parseLong( sleepDurationAsNanoAsString );
            }
            catch ( NumberFormatException e )
            {
                throw new DbException( format( "Error encountered while trying to parse value [%s] for %s",
                        sleepDurationAsNanoAsString, SLEEP_DURATION_NANO_ARG ), e );
            }
        }
        String sleepTypeString = properties.get( SLEEP_TYPE_ARG );
        if ( null == sleepTypeString )
        {
            sleepType = SleepType.SPIN;
        }
        else
        {
            try
            {
                sleepType = SleepType.valueOf( properties.get( SLEEP_TYPE_ARG ) );
            }
            catch ( IllegalArgumentException e )
            {
                throw new DbException( format( "Invalid sleep type: %s", sleepTypeString ) );
            }
        }

        if ( 0 == sleepDurationAsNano )
        {
            sleepFun = new SleepFun()
            {
                @Override
                public void sleep( long sleepNs )
                {
                    // do nothing
                }
            };
        }
        else
        {
            switch ( sleepType )
            {
            case THREAD_SLEEP:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( long sleepNs )
                    {
                        try
                        {
                            Thread.sleep( TimeUnit.NANOSECONDS.toMillis( sleepNs ) );
                        }
                        catch ( InterruptedException e )
                        {
                            // do nothing
                        }
                    }
                };
                break;
            case SPIN:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( long sleepNs )
                    {
                        long endTimeAsNano = System.nanoTime() + sleepNs;
                        while ( System.nanoTime() < endTimeAsNano )
                        {
                            // busy wait
                        }
                    }
                };
                break;
            }
        }

        properties.put( SLEEP_DURATION_NANO_ARG, Long.toString( sleepDurationAsNano ) );
        properties.put( SLEEP_TYPE_ARG, sleepType.name() );

        // Long Reads
        registerOperationHandler( LdbcSnbBiQuery1PostingSummary.class, LdbcQuery1Handler.class );
        registerOperationHandler( LdbcSnbBiQuery2TopTags.class, LdbcQuery2Handler.class );
        registerOperationHandler( LdbcSnbBiQuery3TagEvolution.class, LdbcQuery3Handler.class );
        registerOperationHandler( LdbcSnbBiQuery4PopularCountryTopics.class, LdbcQuery4Handler.class );
        registerOperationHandler( LdbcSnbBiQuery5TopCountryPosters.class, LdbcQuery5Handler.class );
        registerOperationHandler( LdbcSnbBiQuery6ActivePosters.class, LdbcQuery6Handler.class );
        registerOperationHandler( LdbcSnbBiQuery7AuthoritativeUsers.class, LdbcQuery7Handler.class );
        registerOperationHandler( LdbcSnbBiQuery8RelatedTopics.class, LdbcQuery8Handler.class );
        registerOperationHandler( LdbcSnbBiQuery9RelatedForums.class, LdbcQuery9Handler.class );
        registerOperationHandler( LdbcSnbBiQuery10TagPerson.class, LdbcQuery10Handler.class );
        registerOperationHandler( LdbcSnbBiQuery11UnrelatedReplies.class, LdbcQuery11Handler.class );
        registerOperationHandler( LdbcSnbBiQuery12TrendingPosts.class, LdbcQuery12Handler.class );
        registerOperationHandler( LdbcSnbBiQuery13PopularMonthlyTags.class, LdbcQuery13Handler.class );
        registerOperationHandler( LdbcSnbBiQuery14TopThreadInitiators.class, LdbcQuery14Handler.class );
        registerOperationHandler( LdbcSnbBiQuery15SocialNormals.class, LdbcQuery15Handler.class );
        registerOperationHandler( LdbcSnbBiQuery16ExpertsInSocialCircle.class, LdbcQuery16Handler.class );
        registerOperationHandler( LdbcSnbBiQuery17FriendshipTriangles.class, LdbcQuery17Handler.class );
        registerOperationHandler( LdbcSnbBiQuery18PersonPostCounts.class, LdbcQuery18Handler.class );
        registerOperationHandler( LdbcSnbBiQuery19StrangerInteraction.class, LdbcQuery19Handler.class );
        registerOperationHandler( LdbcSnbBiQuery20HighLevelTopics.class, LdbcQuery20Handler.class );
        registerOperationHandler( LdbcSnbBiQuery21Zombies.class, LdbcQuery21Handler.class );
        registerOperationHandler( LdbcSnbBiQuery22InternationalDialog.class, LdbcQuery22Handler.class );
        registerOperationHandler( LdbcSnbBiQuery23HolidayDestinations.class, LdbcQuery23Handler.class );
        registerOperationHandler( LdbcSnbBiQuery24MessagesByTopic.class, LdbcQuery24Handler.class );
    }

    @Override
    protected void onClose() throws IOException
    {
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException
    {
        return null;
    }

    private static void sleep( long sleepNs )
    {
        sleepFun.sleep( sleepNs );
    }

    /*
    LONG READS
     */

    private static final List<LdbcSnbBiQuery1PostingSummaryResult> LDBC_QUERY_1_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read1Results();

    public static class LdbcQuery1Handler
            implements OperationHandler<LdbcSnbBiQuery1PostingSummary,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery1PostingSummary operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_1_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery2TopTagsResult> LDBC_QUERY_2_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read2Results();

    public static class LdbcQuery2Handler implements OperationHandler<LdbcSnbBiQuery2TopTags,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery2TopTags operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_2_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery3TagEvolutionResult> LDBC_QUERY_3_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read3Results();

    public static class LdbcQuery3Handler
            implements OperationHandler<LdbcSnbBiQuery3TagEvolution,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery3TagEvolution operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_3_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery4PopularCountryTopicsResult> LDBC_QUERY_4_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read4Results();

    public static class LdbcQuery4Handler
            implements OperationHandler<LdbcSnbBiQuery4PopularCountryTopics,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery4PopularCountryTopics operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_4_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery5TopCountryPostersResult> LDBC_QUERY_5_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read5Results();

    public static class LdbcQuery5Handler
            implements OperationHandler<LdbcSnbBiQuery5TopCountryPosters,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery5TopCountryPosters operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_5_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery6ActivePostersResult> LDBC_QUERY_6_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read6Results();

    public static class LdbcQuery6Handler
            implements OperationHandler<LdbcSnbBiQuery6ActivePosters,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery6ActivePosters operation,
                DummyDbConnectionState dummyDbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_6_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery7AuthoritativeUsersResult> LDBC_QUERY_7_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read7Results();

    public static class LdbcQuery7Handler
            implements OperationHandler<LdbcSnbBiQuery7AuthoritativeUsers,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery7AuthoritativeUsers operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_7_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery8RelatedTopicsResult> LDBC_QUERY_8_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read8Results();

    public static class LdbcQuery8Handler
            implements OperationHandler<LdbcSnbBiQuery8RelatedTopics,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery8RelatedTopics operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_8_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery9RelatedForumsResult> LDBC_QUERY_9_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read9Results();

    public static class LdbcQuery9Handler
            implements OperationHandler<LdbcSnbBiQuery9RelatedForums,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery9RelatedForums operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_9_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery10TagPersonResult> LDBC_QUERY_10_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read10Results();

    public static class LdbcQuery10Handler implements OperationHandler<LdbcSnbBiQuery10TagPerson,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery10TagPerson operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_10_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery11UnrelatedRepliesResult> LDBC_QUERY_11_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read11Results();

    public static class LdbcQuery11Handler
            implements OperationHandler<LdbcSnbBiQuery11UnrelatedReplies,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery11UnrelatedReplies operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_11_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery12TrendingPostsResult> LDBC_QUERY_12_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read12Results();

    public static class LdbcQuery12Handler
            implements OperationHandler<LdbcSnbBiQuery12TrendingPosts,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery12TrendingPosts operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_12_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery13PopularMonthlyTagsResult> LDBC_QUERY_13_RESULTS =
            DummyLdbcSnbBiOperationResultSets.read13Results();

    public static class LdbcQuery13Handler
            implements OperationHandler<LdbcSnbBiQuery13PopularMonthlyTags,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery13PopularMonthlyTags operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_13_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery14TopThreadInitiatorsResult> LDBC_QUERY_14_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read14Results();

    public static class LdbcQuery14Handler
            implements OperationHandler<LdbcSnbBiQuery14TopThreadInitiators,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery14TopThreadInitiators operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_14_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery15SocialNormalsResult> LDBC_QUERY_15_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read15Results();

    public static class LdbcQuery15Handler
            implements OperationHandler<LdbcSnbBiQuery15SocialNormals,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery15SocialNormals operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_15_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> LDBC_QUERY_16_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read16Results();

    public static class LdbcQuery16Handler
            implements OperationHandler<LdbcSnbBiQuery16ExpertsInSocialCircle,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery16ExpertsInSocialCircle operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_16_RESULTS, operation );
        }
    }

    private static final LdbcSnbBiQuery17FriendshipTrianglesResult LDBC_QUERY_17_RESULTS =
            DummyLdbcSnbBiOperationResultInstances.read17Result();

    public static class LdbcQuery17Handler
            implements OperationHandler<LdbcSnbBiQuery17FriendshipTriangles,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery17FriendshipTriangles operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_17_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery18PersonPostCountsResult> LDBC_QUERY_18_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read18Results();

    public static class LdbcQuery18Handler
            implements OperationHandler<LdbcSnbBiQuery18PersonPostCounts,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery18PersonPostCounts operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_18_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery19StrangerInteractionResult> LDBC_QUERY_19_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read19Results();

    public static class LdbcQuery19Handler
            implements OperationHandler<LdbcSnbBiQuery19StrangerInteraction,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery19StrangerInteraction operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_19_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery20HighLevelTopicsResult> LDBC_QUERY_20_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read20Results();

    public static class LdbcQuery20Handler
            implements OperationHandler<LdbcSnbBiQuery20HighLevelTopics,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery20HighLevelTopics operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_20_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery21ZombiesResult> LDBC_QUERY_21_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read21Results();

    public static class LdbcQuery21Handler implements OperationHandler<LdbcSnbBiQuery21Zombies,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery21Zombies operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_21_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery22InternationalDialogResult> LDBC_QUERY_22_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read22Results();

    public static class LdbcQuery22Handler
            implements OperationHandler<LdbcSnbBiQuery22InternationalDialog,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery22InternationalDialog operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_22_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery23HolidayDestinationsResult> LDBC_QUERY_23_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read23Results();

    public static class LdbcQuery23Handler
            implements OperationHandler<LdbcSnbBiQuery23HolidayDestinations,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery23HolidayDestinations operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_23_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery24MessagesByTopicResult> LDBC_QUERY_24_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read24Results();

    public static class LdbcQuery24Handler
            implements OperationHandler<LdbcSnbBiQuery24MessagesByTopic,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery24MessagesByTopic operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_24_RESULTS, operation );
        }
    }
}
