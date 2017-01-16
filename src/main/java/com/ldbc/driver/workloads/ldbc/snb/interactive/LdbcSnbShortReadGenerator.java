package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.EvictingQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Queues;
import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Tuple;
import com.ldbc.driver.util.Tuple2;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class LdbcSnbShortReadGenerator implements ChildOperationGenerator
{
    private final double initialProbability;
    private final LdbcShortQueryFactory[] shortReadFactories;
    private final double[] probabilityDegradationFactors;
    private final Queue<Long> personIdBuffer;
    private final Queue<Long> messageIdBuffer;
    private final long[] interleavesAsMilli;
    private final BufferReplenishFun bufferReplenishFun;

    public static enum SCHEDULED_START_TIME_POLICY
    {
        PREVIOUS_OPERATION_SCHEDULED_START_TIME,
        PREVIOUS_OPERATION_ACTUAL_FINISH_TIME,
        ESTIMATED
    }

    public LdbcSnbShortReadGenerator( double initialProbability,
            double probabilityDegradationFactor,
            long updateInterleaveAsMilli,
            Set<Class> enabledShortReadOperationTypes,
            double compressionRatio,
            Queue<Long> personIdBuffer,
            Queue<Long> messageIdBuffer,
            RandomDataGeneratorFactory randomFactory,
            Map<Integer,Long> longReadInterleaves,
            SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy,
            BufferReplenishFun bufferReplenishFun )
    {
        this.initialProbability = initialProbability;
        this.personIdBuffer = personIdBuffer;
        this.messageIdBuffer = messageIdBuffer;
        this.bufferReplenishFun = bufferReplenishFun;

        int maxReadOperationType =
                Ordering.<Integer>natural().max( LdbcQuery14.TYPE, LdbcShortQuery7MessageReplies.TYPE ) + 1;
        this.interleavesAsMilli = new long[maxReadOperationType];
        for ( Integer longReadOperationType : longReadInterleaves.keySet() )
        {
            this.interleavesAsMilli[longReadOperationType] =
                    Math.round( Math.ceil( compressionRatio * longReadInterleaves.get( longReadOperationType ) ) );
        }
        this.interleavesAsMilli[LdbcShortQuery1PersonProfile.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );
        this.interleavesAsMilli[LdbcShortQuery2PersonPosts.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );
        this.interleavesAsMilli[LdbcShortQuery3PersonFriends.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );
        this.interleavesAsMilli[LdbcShortQuery4MessageContent.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );
        this.interleavesAsMilli[LdbcShortQuery5MessageCreator.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );
        this.interleavesAsMilli[LdbcShortQuery6MessageForum.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );
        this.interleavesAsMilli[LdbcShortQuery7MessageReplies.TYPE] =
                Math.round( Math.ceil( compressionRatio * updateInterleaveAsMilli ) );

        int maxOperationType = Ordering.<Integer>natural().max( LdbcQuery14.TYPE, LdbcShortQuery7MessageReplies.TYPE,
                LdbcUpdate8AddFriendship.TYPE ) + 1;
        this.shortReadFactories = new LdbcShortQueryFactory[maxOperationType];
        this.probabilityDegradationFactors = new double[maxOperationType];
        for ( int i = 0; i < probabilityDegradationFactors.length; i++ )
        {
            probabilityDegradationFactors[i] = 0;
        }

        /*
        ENABLED
            S1_INDEX -> true/false
            S2_INDEX -> true/false
            S3_INDEX -> true/false
            S4_INDEX -> true/false
            S5_INDEX -> true/false
            S6_INDEX -> true/false
            S7_INDEX -> true/false
         */
        boolean[] enabledShortReads = new boolean[maxOperationType];
        enabledShortReads[LdbcShortQuery1PersonProfile.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery1PersonProfile.class );
        enabledShortReads[LdbcShortQuery2PersonPosts.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery2PersonPosts.class );
        enabledShortReads[LdbcShortQuery3PersonFriends.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery3PersonFriends.class );
        enabledShortReads[LdbcShortQuery4MessageContent.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery4MessageContent.class );
        enabledShortReads[LdbcShortQuery5MessageCreator.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery5MessageCreator.class );
        enabledShortReads[LdbcShortQuery6MessageForum.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery6MessageForum.class );
        enabledShortReads[LdbcShortQuery7MessageReplies.TYPE] =
                enabledShortReadOperationTypes.contains( LdbcShortQuery7MessageReplies.class );

        /*
        MAPPING
            S1_INDEX -> S1
            S2_INDEX -> S2
            S3_INDEX -> S3
            S4_INDEX -> S4
            S5_INDEX -> S5
            S6_INDEX -> S6
            S7_INDEX -> S7
         */
        LdbcShortQueryFactory[] baseShortReadFactories = new LdbcShortQueryFactory[maxOperationType];
        baseShortReadFactories[LdbcShortQuery1PersonProfile.TYPE] =
                new LdbcShortQuery1Factory( scheduledStartTimePolicy );
        baseShortReadFactories[LdbcShortQuery2PersonPosts.TYPE] =
                new LdbcShortQuery2Factory( scheduledStartTimePolicy );
        baseShortReadFactories[LdbcShortQuery3PersonFriends.TYPE] =
                new LdbcShortQuery3Factory( scheduledStartTimePolicy );
        baseShortReadFactories[LdbcShortQuery4MessageContent.TYPE] =
                new LdbcShortQuery4Factory( scheduledStartTimePolicy );
        baseShortReadFactories[LdbcShortQuery5MessageCreator.TYPE] =
                new LdbcShortQuery5Factory( scheduledStartTimePolicy );
        baseShortReadFactories[LdbcShortQuery6MessageForum.TYPE] =
                new LdbcShortQuery6Factory( scheduledStartTimePolicy );
        baseShortReadFactories[LdbcShortQuery7MessageReplies.TYPE] =
                new LdbcShortQuery7Factory( scheduledStartTimePolicy );

        /*
        FACTORIES
            S1_INDEX -> <if> (false == ENABLED[S1]) <then> ERROR
            S2_INDEX -> <if> (false == ENABLED[S2]) <then> ERROR
            S3_INDEX -> <if> (false == ENABLED[S3]) <then> ERROR
            S4_INDEX -> <if> (false == ENABLED[S4]) <then> ERROR
            S5_INDEX -> <if> (false == ENABLED[S5]) <then> ERROR
            S6_INDEX -> <if> (false == ENABLED[S6]) <then> ERROR
            S7_INDEX -> <if> (false == ENABLED[S7]) <then> ERROR
         */
        if ( false == enabledShortReads[LdbcShortQuery1PersonProfile.TYPE] )
        {
            shortReadFactories[LdbcShortQuery1PersonProfile.TYPE] =
                    new ErrorFactory( LdbcShortQuery1PersonProfile.class );
        }
        if ( false == enabledShortReads[LdbcShortQuery2PersonPosts.TYPE] )
        {
            shortReadFactories[LdbcShortQuery2PersonPosts.TYPE] = new ErrorFactory( LdbcShortQuery2PersonPosts.class );
        }
        if ( false == enabledShortReads[LdbcShortQuery3PersonFriends.TYPE] )
        {
            shortReadFactories[LdbcShortQuery3PersonFriends.TYPE] =
                    new ErrorFactory( LdbcShortQuery3PersonFriends.class );
        }
        if ( false == enabledShortReads[LdbcShortQuery4MessageContent.TYPE] )
        {
            shortReadFactories[LdbcShortQuery4MessageContent.TYPE] =
                    new ErrorFactory( LdbcShortQuery4MessageContent.class );
        }
        if ( false == enabledShortReads[LdbcShortQuery5MessageCreator.TYPE] )
        {
            shortReadFactories[LdbcShortQuery5MessageCreator.TYPE] =
                    new ErrorFactory( LdbcShortQuery5MessageCreator.class );
        }
        if ( false == enabledShortReads[LdbcShortQuery6MessageForum.TYPE] )
        {
            shortReadFactories[LdbcShortQuery6MessageForum.TYPE] =
                    new ErrorFactory( LdbcShortQuery6MessageForum.class );
        }
        if ( false == enabledShortReads[LdbcShortQuery7MessageReplies.TYPE] )
        {
            shortReadFactories[LdbcShortQuery7MessageReplies.TYPE] =
                    new ErrorFactory( LdbcShortQuery7MessageReplies.class );
        }

        /*
        (FIRST_PERSON,FIRST_PERSON_INDEX) = ...
        (FIRST_MESSAGE,FIRST_MESSAGE_INDEX) = ...
         */
        Tuple2<Integer,LdbcShortQueryFactory> firstPersonQueryAndIndex =
                firstPersonQueryOrNoOp( enabledShortReadOperationTypes, randomFactory, 0, initialProbability,
                        scheduledStartTimePolicy );
        Tuple2<Integer,LdbcShortQueryFactory> firstMessageQueryAndIndex =
                firstMessageQueryOrNoOp( enabledShortReadOperationTypes, randomFactory, 0, initialProbability,
                        scheduledStartTimePolicy );

        /*
        FIRST_PERSON = <if> (MAX_INTEGER == FIRST_PERSON_INDEX) <then> FIRST_MESSAGE <else> FIRST_PERSON
        FIRST_MESSAGE = <if> (MAX_INTEGER == FIRST_MESSAGE_INDEX) <then> FIRST_PERSON <else> FIRST_MESSAGE
         */
        LdbcShortQueryFactory firstPersonQuery = (Integer.MAX_VALUE == firstPersonQueryAndIndex._1())
                                                 ? firstMessageQueryAndIndex._2()
                                                 : firstPersonQueryAndIndex._2();
        LdbcShortQueryFactory firstMessageQuery = (Integer.MAX_VALUE == firstMessageQueryAndIndex._1())
                                                  ? firstPersonQueryAndIndex._2()
                                                  : firstMessageQueryAndIndex._2();

        /*
        RANDOM_FIRST = RANDOM(FIRST_PERSON,FIRST_MESSAGE)
         */
        LdbcShortQueryFactory randomFirstQuery = selectRandomFirstShortQuery( firstPersonQuery, firstMessageQuery );

        /*
        LAST_PERSON_INDEX = ...
        LAST_MESSAGE_INDEX = ...
         */
        int lastPersonQueryIndex = lastPersonQueryIndex( enabledShortReadOperationTypes );
        int lastMessageQueryIndex = lastMessageQueryIndex( enabledShortReadOperationTypes );

        if ( Integer.MAX_VALUE != lastPersonQueryIndex )
        { probabilityDegradationFactors[lastPersonQueryIndex] = probabilityDegradationFactor; }
        if ( Integer.MAX_VALUE != lastMessageQueryIndex )
        { probabilityDegradationFactors[lastMessageQueryIndex] = probabilityDegradationFactor; }

        /*
        FACTORIES
            [LONG_READ_INDEXES] -> RANDOM_FIRST
            [UPDATE_INDEXES] -> NO_OP
         */
        shortReadFactories[LdbcQuery1.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery2.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery3.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery4.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery5.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery6.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery7.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery8.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery9.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery10.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery11.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery12.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery13.TYPE] = randomFirstQuery;
        shortReadFactories[LdbcQuery14.TYPE] = randomFirstQuery;

        shortReadFactories[LdbcUpdate1AddPerson.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate2AddPostLike.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate3AddCommentLike.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate4AddForum.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate5AddForumMembership.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate6AddPost.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate7AddComment.TYPE] = new NoOpFactory();
        shortReadFactories[LdbcUpdate8AddFriendship.TYPE] = new NoOpFactory();

        shortReadFactories[LdbcShortQuery1PersonProfile.TYPE] = null;
        shortReadFactories[LdbcShortQuery2PersonPosts.TYPE] = null;
        shortReadFactories[LdbcShortQuery3PersonFriends.TYPE] = null;
        shortReadFactories[LdbcShortQuery4MessageContent.TYPE] = null;
        shortReadFactories[LdbcShortQuery5MessageCreator.TYPE] = null;
        shortReadFactories[LdbcShortQuery6MessageForum.TYPE] = null;
        shortReadFactories[LdbcShortQuery7MessageReplies.TYPE] = null;

        /*
        FACTORIES
            <if> (LAST_PERSON_INDEX != MAX_INTEGER) <then>
                LAST_PERSON_INDEX (S1/S2/S3) -> FIRST_MESSAGE
            <if> (LAST_MESSAGE_INDEX != MAX_INTEGER) <then>
                LAST_MESSAGE_INDEX (S4/S5/S6/S7) -> FIRST_PERSON
         */
        if ( Integer.MAX_VALUE != lastPersonQueryIndex )
        { shortReadFactories[lastPersonQueryIndex] = firstMessageQuery; }
        if ( Integer.MAX_VALUE != lastMessageQueryIndex )
        { shortReadFactories[lastMessageQueryIndex] = firstPersonQuery; }

        /*
        FACTORIES
            S1_INDEX -> <if> (ENABLED[S1_INDEX] && UNASSIGNED == FACTORIES[S1_INDEX]) <then>
                            index = indexOfNextEnabledAndUnassigned(MAPPING,S1_INDEX)
                            <if> (index > LAST_PERSON_INDEX) <then> FIRST_MESSAGE <else> MAPPING[index]
            S2_INDEX -> <if> (ENABLED[S2_INDEX] && UNASSIGNED == FACTORIES[S2_INDEX]) <then>
                            index = indexOfNextEnabledAndUnassigned(MAPPING,S2_INDEX)
                            <if> (index > LAST_PERSON_INDEX) <then> FIRST_MESSAGE <else> MAPPING[index]
            S3_INDEX -> // must have already been assigned, or is disabled
            S4_INDEX -> <if> (ENABLED[S4_INDEX] && UNASSIGNED == FACTORIES[S4_INDEX]) <then>
                            index = indexOfNextEnabledAndUnassigned(MAPPING,S4_INDEX)
                            <if> (index > LAST_MESSAGE_INDEX) <then> FIRST_PERSON <else> MAPPING[index]
            S5_INDEX -> <if> (ENABLED[S5_INDEX] && UNASSIGNED == FACTORIES[S5_INDEX]) <then>
                            index = indexOfNextEnabledAndUnassigned(MAPPING,S5_INDEX)
                            <if> (index > LAST_MESSAGE_INDEX) <then> FIRST_PERSON <else> MAPPING[index]
            S6_INDEX -> <if> (ENABLED[S6_INDEX] && UNASSIGNED == FACTORIES[S6_INDEX]) <then>
                            index = indexOfNextEnabledAndUnassigned(MAPPING,S6_INDEX)
                            <if> (index > LAST_MESSAGE_INDEX) <then> FIRST_PERSON <else> MAPPING[index]
            S7_INDEX -> // must have already been assigned, or is disabled
         */
        for ( int i = LdbcShortQuery1PersonProfile.TYPE; i <= LdbcShortQuery3PersonFriends.TYPE; i++ )
        {
            if ( enabledShortReads[i] && null == shortReadFactories[i] )
            {
                int index = indexOfNextEnabled( enabledShortReads, i );
                if ( index > lastPersonQueryIndex )
                { shortReadFactories[i] = firstMessageQuery; }
                else
                { shortReadFactories[i] = baseShortReadFactories[index]; }
            }
        }
        for ( int i = LdbcShortQuery4MessageContent.TYPE; i <= LdbcShortQuery7MessageReplies.TYPE; i++ )
        {
            if ( enabledShortReads[i] && null == shortReadFactories[i] )
            {
                int index = indexOfNextEnabled( enabledShortReads, i );
                if ( index > lastMessageQueryIndex )
                { shortReadFactories[i] = firstPersonQuery; }
                else
                { shortReadFactories[i] = baseShortReadFactories[index]; }
            }
        }
    }

    private int indexOfNextEnabled( boolean[] enabledShortReads, int shortReadType )
    {
        for ( int i = shortReadType + 1; i <= LdbcShortQuery7MessageReplies.TYPE; i++ )
        {
            if ( enabledShortReads[i] )
            { return i; }
        }
        return Integer.MAX_VALUE;
    }

    private Tuple2<Integer,LdbcShortQueryFactory> firstPersonQueryOrNoOp( Set<Class> enabledShortReadOperationTypes,
            RandomDataGeneratorFactory randomFactory,
            double minProbability,
            double maxProbability,
            SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
    {
        if ( enabledShortReadOperationTypes.contains( LdbcShortQuery1PersonProfile.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery1PersonProfile.TYPE,
                    new CoinTossingFactory( randomFactory.newRandom(),
                            new LdbcShortQuery1Factory( scheduledStartTimePolicy ), minProbability, maxProbability )
            );
        }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery2PersonPosts.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery2PersonPosts.TYPE,
                    new CoinTossingFactory( randomFactory.newRandom(),
                            new LdbcShortQuery2Factory( scheduledStartTimePolicy ), minProbability, maxProbability )
            );
        }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery3PersonFriends.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery3PersonFriends.TYPE,
                    new CoinTossingFactory( randomFactory.newRandom(),
                            new LdbcShortQuery3Factory( scheduledStartTimePolicy ), minProbability, maxProbability )
            );
        }
        else
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    Integer.MAX_VALUE,
                    new NoOpFactory()
            );
        }
    }

    private Tuple2<Integer,LdbcShortQueryFactory> firstMessageQueryOrNoOp( Set<Class> enabledShortReadOperationTypes,
            RandomDataGeneratorFactory randomFactory,
            double minProbability,
            double maxProbability,
            SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
    {
        if ( enabledShortReadOperationTypes.contains( LdbcShortQuery4MessageContent.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery4MessageContent.TYPE,
                    new CoinTossingFactory(
                            randomFactory.newRandom(),
                            new LdbcShortQuery4Factory( scheduledStartTimePolicy ),
                            minProbability,
                            maxProbability
                    )
            );
        }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery5MessageCreator.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery5MessageCreator.TYPE,
                    new CoinTossingFactory(
                            randomFactory.newRandom(),
                            new LdbcShortQuery5Factory( scheduledStartTimePolicy ),
                            minProbability,
                            maxProbability
                    )
            );
        }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery6MessageForum.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery6MessageForum.TYPE,
                    new CoinTossingFactory(
                            randomFactory.newRandom(),
                            new LdbcShortQuery6Factory( scheduledStartTimePolicy ),
                            minProbability,
                            maxProbability
                    )
            );
        }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery7MessageReplies.class ) )
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery7MessageReplies.TYPE,
                    new CoinTossingFactory( randomFactory.newRandom(),
                            new LdbcShortQuery7Factory( scheduledStartTimePolicy ), minProbability, maxProbability )
            );
        }
        else
        {
            return Tuple.<Integer,LdbcShortQueryFactory>tuple2(
                    Integer.MAX_VALUE,
                    new NoOpFactory()
            );
        }
    }

    private int lastPersonQueryIndex( Set<Class> enabledShortReadOperationTypes )
    {
        if ( enabledShortReadOperationTypes.contains( LdbcShortQuery3PersonFriends.class ) )
        { return LdbcShortQuery3PersonFriends.TYPE; }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery2PersonPosts.class ) )
        { return LdbcShortQuery2PersonPosts.TYPE; }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery1PersonProfile.class ) )
        { return LdbcShortQuery1PersonProfile.TYPE; }
        else
        { return Integer.MAX_VALUE; }
    }

    private int lastMessageQueryIndex( Set<Class> enabledShortReadOperationTypes )
    {
        if ( enabledShortReadOperationTypes.contains( LdbcShortQuery7MessageReplies.class ) )
        { return LdbcShortQuery7MessageReplies.TYPE; }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery6MessageForum.class ) )
        { return LdbcShortQuery6MessageForum.TYPE; }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery5MessageCreator.class ) )
        { return LdbcShortQuery5MessageCreator.TYPE; }
        else if ( enabledShortReadOperationTypes.contains( LdbcShortQuery4MessageContent.class ) )
        { return LdbcShortQuery4MessageContent.TYPE; }
        else
        { return Integer.MAX_VALUE; }
    }

    private LdbcShortQueryFactory selectRandomFirstShortQuery(
            LdbcShortQueryFactory firstPersonQueryFactory,
            LdbcShortQueryFactory firstMessageQueryFactory )
    {
        if ( firstPersonQueryFactory.describe().equals( firstMessageQueryFactory.describe() ) )
        { return firstPersonQueryFactory; }
        else if ( (false == firstPersonQueryFactory.getClass().equals( NoOpFactory.class )) &&
                  firstMessageQueryFactory.getClass().equals( NoOpFactory.class ) )
        { return firstPersonQueryFactory; }
        else if ( firstPersonQueryFactory.getClass().equals( NoOpFactory.class ) &&
                  (false == firstMessageQueryFactory.getClass().equals( NoOpFactory.class )) )
        { return firstMessageQueryFactory; }
        else
        { return new RoundRobbinFactory( firstPersonQueryFactory, firstMessageQueryFactory ); }
    }

    @Override
    public double initialState()
    {
        return initialProbability;
    }

    @Override
    public Operation nextOperation(
            double state,
            Operation operation,
            Object result,
            long actualStartTimeAsMilli,
            long runDurationAsNano ) throws WorkloadException
    {
        bufferReplenishFun.replenish( operation, result );
        return shortReadFactories[operation.type()].create(
                personIdBuffer,
                messageIdBuffer,
                operation,
                actualStartTimeAsMilli,
                runDurationAsNano,
                state
        );
    }

    @Override
    public double updateState( double previousState, int previousOperationType )
    {
        double degradeBy = probabilityDegradationFactors[previousOperationType];
        return previousState - degradeBy;
    }

    /*
    BufferReplenishFun
     */

    public static interface BufferReplenishFun
    {
        void replenish( Operation operation, Object result );
    }

    public static class NoOpBufferReplenishFun implements BufferReplenishFun
    {
        @Override
        public void replenish( Operation operation, Object result )
        {
        }
    }

    public static class ResultBufferReplenishFun implements BufferReplenishFun
    {
        private final Queue<Long> personIdBuffer;
        private final Queue<Long> messageIdBuffer;

        public ResultBufferReplenishFun( Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer )
        {
            this.personIdBuffer = personIdBuffer;
            this.messageIdBuffer = messageIdBuffer;
        }

        @Override
        public void replenish( Operation operation, Object result )
        {
            switch ( operation.type() )
            {
            case LdbcQuery1.TYPE:
            {
                List<LdbcQuery1Result> typedResults = (List<LdbcQuery1Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    personIdBuffer.add( typedResults.get( i ).friendId() );
                }
                break;
            }
            case LdbcQuery2.TYPE:
            {
                List<LdbcQuery2Result> typedResults = (List<LdbcQuery2Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcQuery2Result typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.personId() );
                    messageIdBuffer.add( typedResult.postOrCommentId() );
                }
                break;
            }
            case LdbcQuery3.TYPE:
            {
                List<LdbcQuery3Result> typedResults = (List<LdbcQuery3Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    personIdBuffer.add( typedResults.get( i ).personId() );
                }
                break;
            }
            case LdbcQuery7.TYPE:
            {
                List<LdbcQuery7Result> typedResults = (List<LdbcQuery7Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcQuery7Result typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.personId() );
                    messageIdBuffer.add( typedResult.commentOrPostId() );
                }
                break;
            }
            case LdbcQuery8.TYPE:
            {
                List<LdbcQuery8Result> typedResults = (List<LdbcQuery8Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcQuery8Result typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.personId() );
                    messageIdBuffer.add( typedResult.commentId() );
                }
                break;
            }
            case LdbcQuery9.TYPE:
            {
                List<LdbcQuery9Result> typedResults = (List<LdbcQuery9Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcQuery9Result typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.personId() );
                    messageIdBuffer.add( typedResult.commentOrPostId() );
                }
                break;
            }
            case LdbcQuery10.TYPE:
            {
                List<LdbcQuery10Result> typedResults = (List<LdbcQuery10Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    personIdBuffer.add( typedResults.get( i ).personId() );
                }
                break;
            }
            case LdbcQuery11.TYPE:
            {
                List<LdbcQuery11Result> typedResults = (List<LdbcQuery11Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    personIdBuffer.add( typedResults.get( i ).personId() );
                }
                break;
            }
            case LdbcQuery12.TYPE:
            {
                List<LdbcQuery12Result> typedResults = (List<LdbcQuery12Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    personIdBuffer.add( typedResults.get( i ).personId() );
                }
                break;
            }
	    case LdbcQuery13.TYPE:
            {
                List<LdbcQuery13Result> typedResults = (List<LdbcQuery13Result>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcQuery13Result typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.personId() );
                    messageIdBuffer.add( typedResult.postOrCommentId() );
                }
                break;
            }
            case LdbcShortQuery2PersonPosts.TYPE:
            {
                List<LdbcShortQuery2PersonPostsResult> typedResults = (List<LdbcShortQuery2PersonPostsResult>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcShortQuery2PersonPostsResult typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.originalPostAuthorId() );
                    messageIdBuffer.add( typedResult.messageId() );
                    messageIdBuffer.add( typedResult.originalPostId() );
                }
                break;
            }
            case LdbcShortQuery3PersonFriends.TYPE:
            {
                List<LdbcShortQuery3PersonFriendsResult> typedResults =
                        (List<LdbcShortQuery3PersonFriendsResult>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    personIdBuffer.add( typedResults.get( i ).personId() );
                }
                break;
            }
            case LdbcShortQuery5MessageCreator.TYPE:
            {
                LdbcShortQuery5MessageCreatorResult typedResult = (LdbcShortQuery5MessageCreatorResult) result;
                personIdBuffer.add( typedResult.personId() );
                break;
            }
            case LdbcShortQuery6MessageForum.TYPE:
            {
                LdbcShortQuery6MessageForumResult typedResult = (LdbcShortQuery6MessageForumResult) result;
                personIdBuffer.add( typedResult.moderatorId() );
                break;
            }
            case LdbcShortQuery7MessageReplies.TYPE:
            {
                List<LdbcShortQuery7MessageRepliesResult> typedResults =
                        (List<LdbcShortQuery7MessageRepliesResult>) result;
                for ( int i = 0; i < typedResults.size(); i++ )
                {
                    LdbcShortQuery7MessageRepliesResult typedResult = typedResults.get( i );
                    personIdBuffer.add( typedResult.replyAuthorId() );
                    messageIdBuffer.add( typedResult.commentId() );
                }
                break;
            }
            }
        }
    }

    /*
    LdbcShortQueryFactory
     */

    private interface LdbcShortQueryFactory
    {
        Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state );

        String describe();
    }

    private class NoOpFactory implements LdbcShortQueryFactory
    {
        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            return null;
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class ErrorFactory implements LdbcShortQueryFactory
    {
        private final Class operationType;

        private ErrorFactory( Class operationType )
        {
            this.operationType = operationType;
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            throw new RuntimeException(
                    format( "Encountered disabled short read: %s - it should not have been executed",
                            operationType.getSimpleName() ) );
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class CoinTossingFactory implements LdbcShortQueryFactory
    {
        private final RandomDataGenerator random;
        private final LdbcShortQueryFactory innerFactory;
        private final double min;
        private final double max;

        private CoinTossingFactory( RandomDataGenerator random,
                LdbcShortQueryFactory innerFactory,
                double min,
                double max )
        {
            this.random = random;
            this.innerFactory = innerFactory;
            this.min = min;
            this.max = max;
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            double coinToss = random.nextUniform( min, max );
            if ( state > coinToss )
            {
                return innerFactory.create(
                        personIdBuffer,
                        messageIdBuffer,
                        previousOperation,
                        previousOperationActualStartTimeAsMilli,
                        previousOperationRunDurationAsNano,
                        state
                );
            }
            else
            { return null; }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName() + "[" + innerFactory.describe() + "]";
        }
    }

    private class RoundRobbinFactory implements LdbcShortQueryFactory
    {
        private final LdbcShortQueryFactory[] innerFactories;
        private final int innerFactoriesCount;
        private int nextFactoryIndex;

        private RoundRobbinFactory( LdbcShortQueryFactory... innerFactories )
        {
            this.innerFactories = innerFactories;
            this.innerFactoriesCount = innerFactories.length;
            this.nextFactoryIndex = -1;
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            nextFactoryIndex = (nextFactoryIndex + 1) % innerFactoriesCount;
            return innerFactories[nextFactoryIndex].create(
                    personIdBuffer,
                    messageIdBuffer,
                    previousOperation,
                    previousOperationActualStartTimeAsMilli,
                    previousOperationRunDurationAsNano,
                    state
            );
        }

        @Override
        public String describe()
        {
            String description = getClass().getSimpleName() + "[";
            if ( innerFactories.length > 0 )
            {
                description += innerFactories[0].describe();
                for ( int i = 1; i < innerFactories.length; i++ )
                {
                    description += "," + innerFactories[i].describe();
                }
            }
            return description + "]";
        }
    }

    private class LdbcShortQuery1Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery1Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = personIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery1PersonProfile( id );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class LdbcShortQuery2Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery2Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = personIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery2PersonPosts( id, LdbcShortQuery2PersonPosts.DEFAULT_LIMIT );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class LdbcShortQuery3Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery3Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = personIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery3PersonFriends( id );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class LdbcShortQuery4Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery4Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = messageIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery4MessageContent( id );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class LdbcShortQuery5Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery5Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = messageIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery5MessageCreator( id );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class LdbcShortQuery6Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery6Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = messageIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery6MessageForum( id );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    private class LdbcShortQuery7Factory implements LdbcShortQueryFactory
    {
        private final ScheduledStartTimeFactory scheduledStartTimeFactory;

        private LdbcShortQuery7Factory( SCHEDULED_START_TIME_POLICY scheduledStartTimePolicy )
        {
            switch ( scheduledStartTimePolicy )
            {
            case ESTIMATED:
            {
                this.scheduledStartTimeFactory = new EstimatedScheduledStartTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_ACTUAL_FINISH_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationActualFinishTimeFactory();
                break;
            }
            case PREVIOUS_OPERATION_SCHEDULED_START_TIME:
            {
                this.scheduledStartTimeFactory = new PreviousOperationScheduledStartTimeFactory();
                break;
            }
            default:
            {
                throw new RuntimeException( format( "Unexpected value, should be one of %s but was %s",
                        Arrays.toString( SCHEDULED_START_TIME_POLICY.values() ),
                        scheduledStartTimePolicy.name() ) );
            }
            }
        }

        @Override
        public Operation create(
                Queue<Long> personIdBuffer,
                Queue<Long> messageIdBuffer,
                Operation previousOperation,
                long previousOperationActualStartTimeAsMilli,
                long previousOperationRunDurationAsNano,
                double state )
        {
            Long id = messageIdBuffer.poll();
            if ( null == id )
            {
                return null;
            }
            else
            {
                Operation operation = new LdbcShortQuery7MessageReplies( id );
                operation.setScheduledStartTimeAsMilli(
                        scheduledStartTimeFactory.nextScheduledStartTime(
                                previousOperation,
                                previousOperationActualStartTimeAsMilli,
                                previousOperationRunDurationAsNano
                        )
                );
                return operation;
            }
        }

        @Override
        public String describe()
        {
            return getClass().getSimpleName();
        }
    }

    /*
    ScheduledStartTimeFactory
     */

    private interface ScheduledStartTimeFactory
    {
        long nextScheduledStartTime( Operation previousOperation, long actualStartTimeAsMilli,
                long previousOperationRunDurationAsNano );
    }

    private class EstimatedScheduledStartTimeFactory implements ScheduledStartTimeFactory
    {
        @Override
        public long nextScheduledStartTime( Operation previousOperation, long actualStartTimeAsMilli,
                long previousOperationRunDurationAsNano )
        {
            return previousOperation.scheduledStartTimeAsMilli() + interleavesAsMilli[previousOperation.type()];
        }
    }

    private class PreviousOperationActualFinishTimeFactory implements ScheduledStartTimeFactory
    {
        @Override
        public long nextScheduledStartTime( Operation previousOperation, long actualStartTimeAsMilli,
                long previousOperationRunDurationAsNano )
        {
            return actualStartTimeAsMilli + TimeUnit.NANOSECONDS.toMillis( previousOperationRunDurationAsNano );
        }
    }

    private class PreviousOperationScheduledStartTimeFactory implements ScheduledStartTimeFactory
    {
        @Override
        public long nextScheduledStartTime( Operation previousOperation, long actualStartTimeAsMilli,
                long previousOperationRunDurationAsNano )
        {
            return previousOperation.scheduledStartTimeAsMilli();
        }
    }

    /*
    Buffer
     */

    static Queue<Long> synchronizedCircularQueueBuffer( int bufferSize )
    {
        return Queues.synchronizedQueue( EvictingQueue.<Long>create( bufferSize ) );
    }

    static Queue<Long> constantBuffer( final long value )
    {
        return new Queue<Long>()
        {
            @Override
            public boolean add( Long aLong )
            {
                return true;
            }

            @Override
            public boolean offer( Long aLong )
            {
                return true;
            }

            @Override
            public Long remove()
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public Long poll()
            {
                return value;
            }

            @Override
            public Long element()
            {
                return value;
            }

            @Override
            public Long peek()
            {
                return value;
            }

            @Override
            public int size()
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean isEmpty()
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean contains( Object o )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public Iterator<Long> iterator()
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public Object[] toArray()
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public <T> T[] toArray( T[] a )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean remove( Object o )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean containsAll( Collection<?> c )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean addAll( Collection<? extends Long> c )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean removeAll( Collection<?> c )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public boolean retainAll( Collection<?> c )
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }

            @Override
            public void clear()
            {
                throw new UnsupportedOperationException( "Method not implemented" );
            }
        };
    }
}
