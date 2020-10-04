package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.ldbc.driver.*;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.workloads.ldbc.snb.bi.*;

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
        registerOperationHandler( LdbcSnbBiQuery2TagEvolution.class, LdbcQuery2Handler.class );
        registerOperationHandler( LdbcSnbBiQuery3PopularCountryTopics.class, LdbcQuery3Handler.class );
        registerOperationHandler( LdbcSnbBiQuery4TopCountryPosters.class, LdbcQuery4Handler.class );
        registerOperationHandler( LdbcSnbBiQuery5ActivePosters.class, LdbcQuery5Handler.class );
        registerOperationHandler( LdbcSnbBiQuery6AuthoritativeUsers.class, LdbcQuery6Handler.class );
        registerOperationHandler( LdbcSnbBiQuery7RelatedTopics.class, LdbcQuery7Handler.class );
        registerOperationHandler( LdbcSnbBiQuery8TagPerson.class, LdbcQuery8Handler.class );
        registerOperationHandler( LdbcSnbBiQuery9TopThreadInitiators.class, LdbcQuery9Handler.class );
        registerOperationHandler( LdbcSnbBiQuery10ExpertsInSocialCircle.class, LdbcQuery10Handler.class );
        registerOperationHandler( LdbcSnbBiQuery11FriendshipTriangles.class, LdbcQuery11Handler.class );
        registerOperationHandler( LdbcSnbBiQuery12PersonPostCounts.class, LdbcQuery12Handler.class );
        registerOperationHandler( LdbcSnbBiQuery13Zombies.class, LdbcQuery13Handler.class );
        registerOperationHandler( LdbcSnbBiQuery14InternationalDialog.class, LdbcQuery14Handler.class );
        registerOperationHandler( LdbcSnbBiQuery15WeightedPaths.class, LdbcQuery15Handler.class );
        registerOperationHandler( LdbcSnbBiQuery18FriendRecommendation.class, LdbcQuery18Handler.class );
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

    private static final List<LdbcSnbBiQuery2TagEvolutionResult> LDBC_QUERY_2_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read2Results();

    public static class LdbcQuery2Handler
            implements OperationHandler<LdbcSnbBiQuery2TagEvolution,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery2TagEvolution operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_2_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery3PopularCountryTopicsResult> LDBC_QUERY_3_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read3Results();

    public static class LdbcQuery3Handler
            implements OperationHandler<LdbcSnbBiQuery3PopularCountryTopics,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery3PopularCountryTopics operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_3_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery4TopCountryPostersResult> LDBC_QUERY_4_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read4Results();

    public static class LdbcQuery4Handler
            implements OperationHandler<LdbcSnbBiQuery4TopCountryPosters,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery4TopCountryPosters operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_4_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery5ActivePostersResult> LDBC_QUERY_5_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read5Results();

    public static class LdbcQuery5Handler
            implements OperationHandler<LdbcSnbBiQuery5ActivePosters,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery5ActivePosters operation,
                DummyDbConnectionState dummyDbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_5_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery6AuthoritativeUsersResult> LDBC_QUERY_6_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read6Results();

    public static class LdbcQuery6Handler
            implements OperationHandler<LdbcSnbBiQuery6AuthoritativeUsers,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery6AuthoritativeUsers operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_6_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery7RelatedTopicsResult> LDBC_QUERY_7_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read7Results();

    public static class LdbcQuery7Handler
            implements OperationHandler<LdbcSnbBiQuery7RelatedTopics,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery7RelatedTopics operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_7_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery8TagPersonResult> LDBC_QUERY_8_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read8Results();

    public static class LdbcQuery8Handler implements OperationHandler<LdbcSnbBiQuery8TagPerson,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery8TagPerson operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_8_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery9TopThreadInitiatorsResult> LDBC_QUERY_9_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read9Results();

    public static class LdbcQuery9Handler
            implements OperationHandler<LdbcSnbBiQuery9TopThreadInitiators,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery9TopThreadInitiators operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_9_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery10ExpertsInSocialCircleResult> LDBC_QUERY_10_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read10Results();

    public static class LdbcQuery10Handler
            implements OperationHandler<LdbcSnbBiQuery10ExpertsInSocialCircle,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery10ExpertsInSocialCircle operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_10_RESULTS, operation );
        }
    }

    private static final LdbcSnbBiQuery11FriendshipTrianglesResult LDBC_QUERY_11_RESULTS =
            DummyLdbcSnbBiOperationResultInstances.read11Result();

    public static class LdbcQuery11Handler
            implements OperationHandler<LdbcSnbBiQuery11FriendshipTriangles,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery11FriendshipTriangles operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_11_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery12PersonPostCountsResult> LDBC_QUERY_12_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read12Results();

    public static class LdbcQuery12Handler
            implements OperationHandler<LdbcSnbBiQuery12PersonPostCounts,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery12PersonPostCounts operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_12_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery13ZombiesResult> LDBC_QUERY_13_RESULTS = DummyLdbcSnbBiOperationResultSets
            .read13Results();

    public static class LdbcQuery13Handler implements OperationHandler<LdbcSnbBiQuery13Zombies,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery13Zombies operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_13_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery14InternationalDialogResult> LDBC_QUERY_14_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read14Results();

    public static class LdbcQuery14Handler
            implements OperationHandler<LdbcSnbBiQuery14InternationalDialog,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery14InternationalDialog operation,
                DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_14_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery15WeightedPathsResult> LDBC_QUERY_15_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read15Results();

    public static class LdbcQuery15Handler
            implements OperationHandler<LdbcSnbBiQuery15WeightedPaths,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery15WeightedPaths operation,
                                      DummyDbConnectionState dbConnectionState,
                                      ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_15_RESULTS, operation );
        }
    }

    private static final List<LdbcSnbBiQuery18FriendRecommendationResult> LDBC_QUERY_18_RESULTS =
            DummyLdbcSnbBiOperationResultSets
                    .read18Results();

    public static class LdbcQuery18Handler
            implements OperationHandler<LdbcSnbBiQuery18FriendRecommendation,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcSnbBiQuery18FriendRecommendation operation,
                                      DummyDbConnectionState dbConnectionState,
                                      ResultReporter resultReporter ) throws DbException
        {
            sleep( sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_18_RESULTS, operation );
        }
    }
}
