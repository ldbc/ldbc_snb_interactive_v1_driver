package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Ordering;
import com.ldbc.driver.ChildOperationGenerator;
import com.ldbc.driver.Operation;
import com.ldbc.driver.WorkloadException;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.util.Tuple;
import org.apache.commons.math3.random.RandomDataGenerator;

import java.util.List;
import java.util.Queue;
import java.util.Set;

public class LdbcSnbShortReadGenerator_NEW implements ChildOperationGenerator {
    private final double initialProbability;
    private final double probabilityDegradationFactor;
    private final LdbcShortQueryFactory[] shortQueryFactories;
    private final long interleaveAsMilli;
    private final Queue<Long> personIdBuffer;
    private final Queue<Long> messageIdBuffer;

    public LdbcSnbShortReadGenerator_NEW(double initialProbability,
                                         double probabilityDegradationFactor,
                                         long interleaveAsMilli,
                                         Set<Class> enabledShortReadOperationTypes,
                                         double compressionRatio,
                                         Queue<Long> personIdBuffer,
                                         Queue<Long> messageIdBuffer) {
        this.initialProbability = initialProbability;
        this.probabilityDegradationFactor = probabilityDegradationFactor;
        this.personIdBuffer = personIdBuffer;
        this.messageIdBuffer = messageIdBuffer;
        this.shortQueryFactories = buildLdbcShortQueryFactories(enabledShortReadOperationTypes, new RandomDataGeneratorFactory(42l));
        this.interleaveAsMilli = Math.round(Math.ceil(compressionRatio * interleaveAsMilli));
    }

    private LdbcShortQueryFactory[] buildLdbcShortQueryFactories(Set<Class> enabledShortReadOperationTypes,
                                                                 RandomDataGeneratorFactory randomFactory) {
        Tuple.Tuple2<Integer, LdbcShortQueryFactory> firstPersonQuery = firstPersonQueryOrNoOp(enabledShortReadOperationTypes, randomFactory);
        Tuple.Tuple2<Integer, LdbcShortQueryFactory> firstMessageQuery = firstMessageQueryOrNoOp(enabledShortReadOperationTypes, randomFactory);
        LdbcShortQueryFactory randomFirstQuery = selectRandomFirstShortQuery(firstPersonQuery._2(), firstMessageQuery._2());

        int maxOperationType = Ordering.<Integer>natural().max(LdbcQuery14.TYPE, LdbcShortQuery7MessageReplies.TYPE);
        LdbcShortQueryFactory[] ldbcShortQueryFactories = new LdbcShortQueryFactory[maxOperationType + 1];

        ldbcShortQueryFactories[LdbcQuery1.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery2.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery3.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery4.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery5.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery6.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery7.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery8.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery9.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery10.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery11.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery12.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery13.TYPE] = randomFirstQuery;
        ldbcShortQueryFactories[LdbcQuery14.TYPE] = randomFirstQuery;

        ldbcShortQueryFactories[LdbcUpdate1AddPerson.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate2AddPostLike.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate3AddCommentLike.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate4AddForum.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate5AddForumMembership.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate6AddPost.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate7AddComment.TYPE] = new NoOpFactory();
        ldbcShortQueryFactories[LdbcUpdate8AddFriendship.TYPE] = new NoOpFactory();

        /*
        ENABLED
            S1_INDEX -> true/false
            S2_INDEX -> true/false
            S3_INDEX -> true/false
            S4_INDEX -> true/false
            S5_INDEX -> true/false
            S6_INDEX -> true/false
            S7_INDEX -> true/false

        MAPPING
            S1_INDEX -> S1
            S2_INDEX -> S2
            S3_INDEX -> S3
            S4_INDEX -> S4
            S5_INDEX -> S5
            S6_INDEX -> S6
            S7_INDEX -> S7

        FACTORIES
            S1_INDEX -> <if> (false == ENABLED[S1]) <then> ERROR
            S2_INDEX -> <if> (false == ENABLED[S2]) <then> ERROR
            S3_INDEX -> <if> (false == ENABLED[S3]) <then> ERROR
            S4_INDEX -> <if> (false == ENABLED[S4]) <then> ERROR
            S5_INDEX -> <if> (false == ENABLED[S5]) <then> ERROR
            S6_INDEX -> <if> (false == ENABLED[S6]) <then> ERROR
            S7_INDEX -> <if> (false == ENABLED[S7]) <then> ERROR

        (FIRST_PERSON,FIRST_PERSON_INDEX) = ...
        (FIRST_MESSAGE,FIRST_MESSAGE_INDEX) = ...
        RANDOM_FIRST = RANDOM(FIRST_PERSON,FIRST_MESSAGE)

        FIRST_PERSON = <if> (MAX_INTEGER == FIRST_PERSON_INDEX) <then> FIRST_MESSAGE <else> FIRST_PERSON
        FIRST_MESSAGE = <if> (MAX_INTEGER == FIRST_MESSAGE_INDEX) <then> FIRST_PERSON <else> FIRST_MESSAGE

        LAST_PERSON_INDEX = ...
        LAST_MESSAGE_INDEX = ...

        FACTORIES
            [LONG_READ_INDEXES] -> RANDOM_FIRST
            [UPDATE_INDEXES] -> NO_OP

        FACTORIES
            <if> (LAST_PERSON_INDEX != MAX_INTEGER) <then>
                LAST_PERSON_INDEX (S1/S2/S3) -> FIRST_MESSAGE
            <if> (LAST_MESSAGE_INDEX != MAX_INTEGER) <then>
                LAST_MESSAGE_INDEX (S4/S5/S6/S7) -> FIRST_PERSON

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
        // TODO uncomment
//        ldbcShortQueryFactories[LdbcShortQuery3PersonFriends.TYPE] = (firstMessageQuery._2().getClass().equals(NoOpFactory.class))
//                ? firstPersonQuery._2()
//                : firstMessageQuery._2();
//        ldbcShortQueryFactories[LdbcShortQuery2PersonPosts.TYPE] = (enabledShortReadOperationTypes.contains(LdbcShortQuery3PersonFriends.class))
//                ? new LdbcShortQuery3Factory()
//                :
//                ldbcShortQueryFactories[LdbcShortQuery1PersonProfile.TYPE] = new NoOpFactory(); // TODO 2/3/null

        ldbcShortQueryFactories[LdbcShortQuery7MessageReplies.TYPE] = (firstPersonQuery._2().getClass().equals(NoOpFactory.class))
                ? firstMessageQuery._2()
                : firstPersonQuery._2();

        ldbcShortQueryFactories[LdbcShortQuery4MessageContent.TYPE] = new NoOpFactory(); // TODO 5/6/7/null
        ldbcShortQueryFactories[LdbcShortQuery5MessageCreator.TYPE] = new NoOpFactory(); // TODO 6/7/null
        ldbcShortQueryFactories[LdbcShortQuery6MessageForum.TYPE] = new NoOpFactory(); // TODO 7/null

        return ldbcShortQueryFactories;
    }

    private Tuple.Tuple2<Integer, LdbcShortQueryFactory> firstPersonQueryOrNoOp(Set<Class> enabledShortReadOperationTypes,
                                                                                RandomDataGeneratorFactory randomFactory) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery1PersonProfile.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery1PersonProfile.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery1Factory())
            );
        else if (enabledShortReadOperationTypes.contains(LdbcShortQuery2PersonPosts.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery2PersonPosts.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery2Factory())
            );
        else if (enabledShortReadOperationTypes.contains(LdbcShortQuery3PersonFriends.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery3PersonFriends.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery3Factory())
            );
        else
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    Integer.MAX_VALUE,
                    new NoOpFactory()
            );
    }

    private Tuple.Tuple2<Integer, LdbcShortQueryFactory> firstMessageQueryOrNoOp(Set<Class> enabledShortReadOperationTypes,
                                                                                 RandomDataGeneratorFactory randomFactory) {
        if (enabledShortReadOperationTypes.contains(LdbcShortQuery4MessageContent.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery4MessageContent.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery4Factory())
            );
        else if (enabledShortReadOperationTypes.contains(LdbcShortQuery5MessageCreator.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery5MessageCreator.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery5Factory())
            );
        else if (enabledShortReadOperationTypes.contains(LdbcShortQuery6MessageForum.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery6MessageForum.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery6Factory())
            );
        else if (enabledShortReadOperationTypes.contains(LdbcShortQuery7MessageReplies.class))
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    LdbcShortQuery7MessageReplies.TYPE,
                    new CoinTossingFactory(randomFactory.newRandom(), new LdbcShortQuery7Factory())
            );
        else
            return Tuple.<Integer, LdbcShortQueryFactory>tuple2(
                    Integer.MAX_VALUE,
                    new NoOpFactory()
            );
    }

    private LdbcShortQueryFactory selectRandomFirstShortQuery(LdbcShortQueryFactory firstPersonQueryFactory,
                                                              LdbcShortQueryFactory firstMessageQueryFactory) {
        if (firstPersonQueryFactory.getClass().equals(NoOpFactory.class) && firstMessageQueryFactory.getClass().equals(NoOpFactory.class))
            return new NoOpFactory();
        else if ((false == firstPersonQueryFactory.getClass().equals(NoOpFactory.class)) && firstMessageQueryFactory.getClass().equals(NoOpFactory.class))
            return firstPersonQueryFactory;
        else if (firstPersonQueryFactory.getClass().equals(NoOpFactory.class) && (false == firstMessageQueryFactory.getClass().equals(NoOpFactory.class)))
            return firstMessageQueryFactory;
        else
            return new RoundRobbinFactory(firstPersonQueryFactory, firstMessageQueryFactory);
    }

    @Override
    public double initialState() {
        return initialProbability;
    }

    @Override
    public Operation<?> nextOperation(double state, Operation operation, Object result) throws WorkloadException {
        switch (operation.type()) {
            case LdbcQuery1.TYPE: {
                List<LdbcQuery1Result> typedResults = (List<LdbcQuery1Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).friendId());
                }
            }
            case LdbcQuery2.TYPE: {
                List<LdbcQuery2Result> typedResults = (List<LdbcQuery2Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    LdbcQuery2Result typedResult = typedResults.get(i);
                    personIdBuffer.add(typedResult.personId());
                    messageIdBuffer.add(typedResult.postOrCommentId());
                }
            }
            case LdbcQuery3.TYPE: {
                List<LdbcQuery3Result> typedResults = (List<LdbcQuery3Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).personId());
                }
            }
            case LdbcQuery7.TYPE: {
                List<LdbcQuery7Result> typedResults = (List<LdbcQuery7Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    LdbcQuery7Result typedResult = typedResults.get(i);
                    personIdBuffer.add(typedResult.personId());
                    messageIdBuffer.add(typedResult.commentOrPostId());
                }
            }
            case LdbcQuery8.TYPE: {
                List<LdbcQuery8Result> typedResults = (List<LdbcQuery8Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    LdbcQuery8Result typedResult = typedResults.get(i);
                    personIdBuffer.add(typedResult.personId());
                    messageIdBuffer.add(typedResult.commentId());
                }
            }
            case LdbcQuery9.TYPE: {
                List<LdbcQuery9Result> typedResults = (List<LdbcQuery9Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    LdbcQuery9Result typedResult = typedResults.get(i);
                    personIdBuffer.add(typedResult.personId());
                    messageIdBuffer.add(typedResult.commentOrPostId());
                }
            }
            case LdbcQuery10.TYPE: {
                List<LdbcQuery10Result> typedResults = (List<LdbcQuery10Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).personId());
                }
            }
            case LdbcQuery11.TYPE: {
                List<LdbcQuery11Result> typedResults = (List<LdbcQuery11Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).personId());
                }
            }
            case LdbcQuery12.TYPE: {
                List<LdbcQuery12Result> typedResults = (List<LdbcQuery12Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).personId());
                }
            }
            case LdbcQuery14.TYPE: {
                List<LdbcQuery14Result> typedResults = (List<LdbcQuery14Result>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    for (Number personId : typedResults.get(i).personsIdsInPath()) {
                        personIdBuffer.add(personId.longValue());
                    }
                }
            }
            case LdbcShortQuery2PersonPosts.TYPE: {
                List<LdbcShortQuery2PersonPostsResult> typedResults = (List<LdbcShortQuery2PersonPostsResult>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    LdbcShortQuery2PersonPostsResult typedResult = typedResults.get(i);
                    personIdBuffer.add(typedResult.originalPostAuthorId());
                    messageIdBuffer.add(typedResult.messageId());
                    messageIdBuffer.add(typedResult.originalPostId());
                }
            }
            case LdbcShortQuery3PersonFriends.TYPE: {
                List<LdbcShortQuery3PersonFriendsResult> typedResults = (List<LdbcShortQuery3PersonFriendsResult>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).personId());
                }
            }
            case LdbcShortQuery5MessageCreator.TYPE: {
                List<LdbcShortQuery5MessageCreatorResult> typedResults = (List<LdbcShortQuery5MessageCreatorResult>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    personIdBuffer.add(typedResults.get(i).personId());
                }
            }
            case LdbcShortQuery6MessageForum.TYPE: {
                LdbcShortQuery6MessageForumResult typedResult = (LdbcShortQuery6MessageForumResult) result;
                personIdBuffer.add(typedResult.moderatorId());
            }
            case LdbcShortQuery7MessageReplies.TYPE: {
                List<LdbcShortQuery7MessageRepliesResult> typedResults = (List<LdbcShortQuery7MessageRepliesResult>) result;
                for (int i = 0; i < typedResults.size(); i++) {
                    LdbcShortQuery7MessageRepliesResult typedResult = typedResults.get(i);
                    personIdBuffer.add(typedResult.replyAuthorId());
                    messageIdBuffer.add(typedResult.commentId());
                }
            }
        }
        return shortQueryFactories[operation.type()].create(personIdBuffer, messageIdBuffer, operation.scheduledStartTimeAsMilli(), state);
    }

    @Override
    public double updateState(double previousState) {
        return previousState - probabilityDegradationFactor;
    }

    private interface LdbcShortQueryFactory {
        Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state);
    }

    private class NoOpFactory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            return null;
        }
    }

    private class CoinTossingFactory implements LdbcShortQueryFactory {
        private final RandomDataGenerator random;
        private final LdbcShortQueryFactory innerFactory;

        private CoinTossingFactory(RandomDataGenerator random, LdbcShortQueryFactory innerFactory) {
            this.random = random;
            this.innerFactory = innerFactory;
        }

        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            if (state > random.nextUniform(0, 1))
                return innerFactory.create(personIdBuffer, messageIdBuffer, previousScheduledStartTime, state);
            else
                return null;
        }
    }

    private class RoundRobbinFactory implements LdbcShortQueryFactory {
        private final LdbcShortQueryFactory[] innerFactories;
        private final int innerFactoriesCount;
        private int nextFactoryIndex;

        private RoundRobbinFactory(LdbcShortQueryFactory... innerFactories) {
            this.innerFactories = innerFactories;
            this.innerFactoriesCount = innerFactories.length;
            this.nextFactoryIndex = 0;
        }

        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            nextFactoryIndex = (nextFactoryIndex + 1) % innerFactoriesCount;
            return innerFactories[nextFactoryIndex].create(personIdBuffer, messageIdBuffer, previousScheduledStartTime, state);
        }
    }

    private class LdbcShortQuery1Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = personIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery1PersonProfile(id);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }

    private class LdbcShortQuery2Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = personIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery2PersonPosts(id, LdbcShortQuery2PersonPosts.DEFAULT_LIMIT);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }

    private class LdbcShortQuery3Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = personIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery3PersonFriends(id);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }

    private class LdbcShortQuery4Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = messageIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery4MessageContent(id);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }

    private class LdbcShortQuery5Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = messageIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery5MessageCreator(id);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }

    private class LdbcShortQuery6Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = messageIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery6MessageForum(id);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }

    private class LdbcShortQuery7Factory implements LdbcShortQueryFactory {
        @Override
        public Operation create(Queue<Long> personIdBuffer, Queue<Long> messageIdBuffer, long previousScheduledStartTime, double state) {
            Long id = messageIdBuffer.poll();
            if (null == id) {
                return null;
            } else {
                Operation operation = new LdbcShortQuery7MessageReplies(id);
                operation.setScheduledStartTimeAsMilli(previousScheduledStartTime + interleaveAsMilli);
                return operation;
            }
        }
    }
}