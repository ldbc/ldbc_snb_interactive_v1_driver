package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.Operation;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.control.LoggingService;
import com.ldbc.driver.util.ClassLoaderHelper;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcNoResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery10Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery11Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery12Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery13Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery14Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery2Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery3Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery4Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery5Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery6Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery7Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery8Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcQuery9Result;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfile;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery1PersonProfileResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPosts;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery2PersonPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriends;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery3PersonFriendsResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContent;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery4MessageContentResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreator;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery5MessageCreatorResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery6MessageForumResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageReplies;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcShortQuery7MessageRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate1AddPerson;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate3AddCommentLike;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate4AddForum;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate6AddPost;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate7AddComment;
import com.ldbc.driver.workloads.ldbc.snb.interactive.LdbcUpdate8AddFriendship;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.lang.String.format;

public class DummyLdbcSnbInteractiveDb extends Db
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
        SLEEP,
        PARK,
        SPIN
    }

    public static final String SLEEP_DURATION_NANO_ARG = "ldbc.snb.interactive.db.sleep_duration_nano";
    public static final String SLEEP_TYPE_ARG = "ldbc.snb.interactive.db.sleep_type";
    public static final String CRASH_ON_ARG = "ldbc.snb.interactive.db.crash_on";

    private static long sleepDurationAsNano;
    private SleepType sleepType;
    private Class crashOnClass;

    private interface SleepFun
    {
        void sleep( Operation operation, long sleepNs );
    }

    private static SleepFun sleepFun;

    @Override
    protected void onInit( Map<String,String> params, LoggingService loggingService ) throws DbException
    {
        String sleepDurationAsNanoAsString = params.get( SLEEP_DURATION_NANO_ARG );
        try
        {
            crashOnClass = (params.containsKey( CRASH_ON_ARG ))
                           ? ClassLoaderHelper.loadClass( params.get( CRASH_ON_ARG ) )
                           : null;
        }
        catch ( Exception e )
        {
            throw new DbException( format( "Error loading operation class: %s", params.get( CRASH_ON_ARG ) ), e );
        }
        if ( null == sleepDurationAsNanoAsString )
        {
            sleepDurationAsNano = 0L;
        }
        else
        {
            try
            {
                sleepDurationAsNano = Long.parseLong( sleepDurationAsNanoAsString );
            }
            catch ( NumberFormatException e )
            {
                throw new DbException( format( "Error encountered while trying to parse value [%s] for argument [%s]",
                        sleepDurationAsNanoAsString, SLEEP_DURATION_NANO_ARG ), e );
            }
        }
        String sleepTypeString = params.get( SLEEP_TYPE_ARG );
        if ( null == sleepTypeString )
        {
            sleepType = SleepType.SPIN;
        }
        else
        {
            try
            {
                sleepType = SleepType.valueOf( params.get( SLEEP_TYPE_ARG ) );
            }
            catch ( IllegalArgumentException e )
            {
                throw new DbException( format( "Invalid sleep type: %s", sleepTypeString ) );
            }
        }

        if ( null != crashOnClass )
        {
            sleepFun = new SleepFun()
            {
                @Override
                public void sleep( Operation operation, long sleepNs )
                {
                    if ( crashOnClass.equals( operation.getClass() ) )
                    {
                        throw new RuntimeException( "Crash on purpose" );
                    }
                }
            };
        }
        else if ( 0 == sleepDurationAsNano )
        {
            sleepFun = new SleepFun()
            {
                @Override
                public void sleep( Operation operation, long sleepNs )
                {
                    // do nothing
                }
            };
        }
        else
        {
            switch ( sleepType )
            {
            case SLEEP:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( Operation operation, long sleepNs )
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
            case PARK:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( Operation operation, long sleepNs )
                    {
                        LockSupport.parkNanos( sleepNs );
                    }
                };
                break;
            case SPIN:
                sleepFun = new SleepFun()
                {
                    @Override
                    public void sleep( Operation operation, long sleepNs )
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

        params.put( SLEEP_DURATION_NANO_ARG, Long.toString( sleepDurationAsNano ) );
        params.put( SLEEP_TYPE_ARG, sleepType.name() );

        // Long Reads
        registerOperationHandler( LdbcQuery1.class, LdbcQuery1Handler.class );
        registerOperationHandler( LdbcQuery2.class, LdbcQuery2Handler.class );
        registerOperationHandler( LdbcQuery3.class, LdbcQuery3Handler.class );
        registerOperationHandler( LdbcQuery4.class, LdbcQuery4Handler.class );
        registerOperationHandler( LdbcQuery5.class, LdbcQuery5Handler.class );
        registerOperationHandler( LdbcQuery6.class, LdbcQuery6Handler.class );
        registerOperationHandler( LdbcQuery7.class, LdbcQuery7Handler.class );
        registerOperationHandler( LdbcQuery8.class, LdbcQuery8Handler.class );
        registerOperationHandler( LdbcQuery9.class, LdbcQuery9Handler.class );
        registerOperationHandler( LdbcQuery10.class, LdbcQuery10Handler.class );
        registerOperationHandler( LdbcQuery11.class, LdbcQuery11Handler.class );
        registerOperationHandler( LdbcQuery12.class, LdbcQuery12Handler.class );
        registerOperationHandler( LdbcQuery13.class, LdbcQuery13Handler.class );
        registerOperationHandler( LdbcQuery14.class, LdbcQuery14Handler.class );
        // Short Reads
        registerOperationHandler( LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class );
        registerOperationHandler( LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class );
        registerOperationHandler( LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class );
        registerOperationHandler( LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class );
        registerOperationHandler( LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class );
        registerOperationHandler( LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class );
        registerOperationHandler( LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class );
        // Updates
        registerOperationHandler( LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class );
        registerOperationHandler( LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class );
        registerOperationHandler( LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class );
        registerOperationHandler( LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class );
        registerOperationHandler( LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class );
        registerOperationHandler( LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class );
        registerOperationHandler( LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class );
        registerOperationHandler( LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class );
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

    private static void sleep( Operation operation, long sleepNs )
    {
        sleepFun.sleep( operation, sleepNs );
    }

    /*
    LONG READS
     */

    private static final List<LdbcQuery1Result> LDBC_QUERY_1_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read1Results();

    public static class LdbcQuery1Handler implements OperationHandler<LdbcQuery1,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery1 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_1_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery2Result> LDBC_QUERY_2_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read2Results();

    public static class LdbcQuery2Handler implements OperationHandler<LdbcQuery2,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery2 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_2_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery3Result> LDBC_QUERY_3_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read3Results();

    public static class LdbcQuery3Handler implements OperationHandler<LdbcQuery3,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery3 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_3_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery4Result> LDBC_QUERY_4_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read4Results();

    public static class LdbcQuery4Handler implements OperationHandler<LdbcQuery4,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery4 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_4_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery5Result> LDBC_QUERY_5_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read5Results();

    public static class LdbcQuery5Handler implements OperationHandler<LdbcQuery5,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery5 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_5_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery6Result> LDBC_QUERY_6_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read6Results();

    public static class LdbcQuery6Handler implements OperationHandler<LdbcQuery6,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery6 operation, DummyDbConnectionState dummyDbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_6_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery7Result> LDBC_QUERY_7_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read7Results();

    public static class LdbcQuery7Handler implements OperationHandler<LdbcQuery7,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery7 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_7_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery8Result> LDBC_QUERY_8_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read8Results();

    public static class LdbcQuery8Handler implements OperationHandler<LdbcQuery8,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery8 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_8_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery9Result> LDBC_QUERY_9_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read9Results();

    public static class LdbcQuery9Handler implements OperationHandler<LdbcQuery9,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery9 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_9_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery10Result> LDBC_QUERY_10_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read10Results();

    public static class LdbcQuery10Handler implements OperationHandler<LdbcQuery10,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery10 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_10_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery11Result> LDBC_QUERY_11_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read11Results();

    public static class LdbcQuery11Handler implements OperationHandler<LdbcQuery11,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery11 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_11_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery12Result> LDBC_QUERY_12_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read12Results();

    public static class LdbcQuery12Handler implements OperationHandler<LdbcQuery12,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery12 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_12_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery13Result> LDBC_QUERY_13_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read13Results();

    public static class LdbcQuery13Handler implements OperationHandler<LdbcQuery13,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery13 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_13_RESULTS, operation );
        }
    }

    private static final List<LdbcQuery14Result> LDBC_QUERY_14_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.read14Results();

    public static class LdbcQuery14Handler implements OperationHandler<LdbcQuery14,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcQuery14 operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_QUERY_14_RESULTS, operation );
        }
    }

    /*
    SHORT READS
     */

    private static final LdbcShortQuery1PersonProfileResult LDBC_SHORT_QUERY_1_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short1Results();

    public static class LdbcShortQuery1PersonProfileHandler
            implements OperationHandler<LdbcShortQuery1PersonProfile,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery1PersonProfile operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_1_RESULTS, operation );
        }
    }

    private static final List<LdbcShortQuery2PersonPostsResult> LDBC_SHORT_QUERY_2_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short2Results();

    public static class LdbcShortQuery2PersonPostsHandler
            implements OperationHandler<LdbcShortQuery2PersonPosts,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery2PersonPosts operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_2_RESULTS, operation );
        }
    }

    private static final List<LdbcShortQuery3PersonFriendsResult> LDBC_SHORT_QUERY_3_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short3Results();

    public static class LdbcShortQuery3PersonFriendsHandler
            implements OperationHandler<LdbcShortQuery3PersonFriends,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery3PersonFriends operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_3_RESULTS, operation );
        }
    }

    private static final LdbcShortQuery4MessageContentResult LDBC_SHORT_QUERY_4_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short4Results();

    public static class LdbcShortQuery4MessageContentHandler
            implements OperationHandler<LdbcShortQuery4MessageContent,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery4MessageContent operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_4_RESULTS, operation );
        }
    }

    private static final LdbcShortQuery5MessageCreatorResult LDBC_SHORT_QUERY_5_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short5Results();

    public static class LdbcShortQuery5MessageCreatorHandler
            implements OperationHandler<LdbcShortQuery5MessageCreator,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery5MessageCreator operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_5_RESULTS, operation );
        }
    }

    private static final LdbcShortQuery6MessageForumResult LDBC_SHORT_QUERY_6_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short6Results();

    public static class LdbcShortQuery6MessageForumHandler
            implements OperationHandler<LdbcShortQuery6MessageForum,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery6MessageForum operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_6_RESULTS, operation );
        }
    }

    private static final List<LdbcShortQuery7MessageRepliesResult> LDBC_SHORT_QUERY_7_RESULTS =
            DummyLdbcSnbInteractiveOperationResultSets.short7Results();

    public static class LdbcShortQuery7MessageRepliesHandler
            implements OperationHandler<LdbcShortQuery7MessageReplies,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcShortQuery7MessageReplies operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LDBC_SHORT_QUERY_7_RESULTS, operation );
        }
    }

    /*
    UPDATES
     */

    public static class LdbcUpdate1AddPersonHandler
            implements OperationHandler<LdbcUpdate1AddPerson,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate1AddPerson operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate2AddPostLikeHandler
            implements OperationHandler<LdbcUpdate2AddPostLike,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate2AddPostLike operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate3AddCommentLikeHandler
            implements OperationHandler<LdbcUpdate3AddCommentLike,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate3AddCommentLike operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate4AddForumHandler
            implements OperationHandler<LdbcUpdate4AddForum,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate4AddForum operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate5AddForumMembershipHandler
            implements OperationHandler<LdbcUpdate5AddForumMembership,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate5AddForumMembership operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate6AddPost operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate7AddCommentHandler
            implements OperationHandler<LdbcUpdate7AddComment,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate7AddComment operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }

    public static class LdbcUpdate8AddFriendshipHandler
            implements OperationHandler<LdbcUpdate8AddFriendship,DummyDbConnectionState>
    {
        @Override
        public void executeOperation( LdbcUpdate8AddFriendship operation, DummyDbConnectionState dbConnectionState,
                ResultReporter resultReporter ) throws DbException
        {
            sleep( operation, sleepDurationAsNano );
            resultReporter.report( 0, LdbcNoResult.INSTANCE, operation );
        }
    }
}
