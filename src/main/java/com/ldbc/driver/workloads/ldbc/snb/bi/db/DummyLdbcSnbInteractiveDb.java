package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.ldbc.driver.Db;
import com.ldbc.driver.DbConnectionState;
import com.ldbc.driver.DbException;
import com.ldbc.driver.OperationHandler;
import com.ldbc.driver.ResultReporter;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1Result;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcNoResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class DummyLdbcSnbInteractiveDb extends Db {
    private static class DummyDbConnectionState extends DbConnectionState {
        @Override
        public void close() throws IOException {
        }
    }

    public static enum SleepType {
        THREAD_SLEEP,
        SPIN
    }

    public static final String SLEEP_DURATION_NANO_ARG = "ldbc.snb.interactive.db.sleep_duration_nano";
    public static final String SLEEP_TYPE_ARG = "ldbc.snb.interactive.db.sleep_type";

    private static long sleepDurationAsNano;
    private SleepType sleepType;

    private static interface SleepFun {
        void sleep( long sleepNs );
    }

    private static SleepFun sleepFun;

    @Override
    protected void onInit(Map<String, String> properties) throws DbException {
        String sleepDurationAsNanoAsString = properties.get(SLEEP_DURATION_NANO_ARG);
        if (null == sleepDurationAsNanoAsString) {
            sleepDurationAsNano = 0l;
        } else {
            try {
                sleepDurationAsNano = Long.parseLong(sleepDurationAsNanoAsString);
            } catch (NumberFormatException e) {
                throw new DbException(String.format("Error encountered while trying to parse value [%s] for %s", sleepDurationAsNanoAsString, SLEEP_DURATION_NANO_ARG), e);
            }
        }
        String sleepTypeString = properties.get(SLEEP_TYPE_ARG);
        if (null == sleepTypeString) {
            sleepType = SleepType.SPIN;
        } else {
            try {
                sleepType = SleepType.valueOf( properties.get( SLEEP_TYPE_ARG ) );
            } catch (IllegalArgumentException e) {
                throw new DbException(String.format("Invalid sleep type: %s", sleepTypeString));
            }
        }

        if (0 == sleepDurationAsNano) {
            sleepFun = new SleepFun() {
                @Override
                public void sleep(long sleepNs) {
                    // do nothing
                }
            };
        } else {
            switch (sleepType) {
                case THREAD_SLEEP:
                    sleepFun = new SleepFun() {
                        @Override
                        public void sleep(long sleepNs) {
                            try {
                                Thread.sleep(TimeUnit.NANOSECONDS.toMillis(sleepNs));
                            } catch (InterruptedException e) {
                                // do nothing
                            }
                        }
                    };
                    break;
                case SPIN:
                    sleepFun = new SleepFun() {
                        @Override
                        public void sleep(long sleepNs) {
                            long endTimeAsNano = System.nanoTime() + sleepNs;
                            while (System.nanoTime() < endTimeAsNano) {
                                // busy wait
                            }
                        }
                    };
                    break;
            }
        }

        properties.put(SLEEP_DURATION_NANO_ARG, Long.toString(sleepDurationAsNano));
        properties.put(SLEEP_TYPE_ARG, sleepType.name());

        // Long Reads
        registerOperationHandler(LdbcSnbBiQuery1.class, LdbcQuery1Handler.class);
        registerOperationHandler(LdbcSnbBiQuery2.class, LdbcQuery2Handler.class);
        registerOperationHandler(LdbcSnbBiQuery3.class, LdbcQuery3Handler.class);
        registerOperationHandler(LdbcSnbBiQuery4.class, LdbcQuery4Handler.class);
        registerOperationHandler(LdbcSnbBiQuery5.class, LdbcQuery5Handler.class);
        registerOperationHandler(LdbcSnbBiQuery6.class, LdbcQuery6Handler.class);
        registerOperationHandler(LdbcSnbBiQuery7.class, LdbcQuery7Handler.class);
        registerOperationHandler(LdbcSnbBiQuery8.class, LdbcQuery8Handler.class);
        registerOperationHandler(LdbcSnbBiQuery9.class, LdbcQuery9Handler.class);
        registerOperationHandler(LdbcSnbBiQuery10.class, LdbcQuery10Handler.class);
        registerOperationHandler(LdbcSnbBiQuery11.class, LdbcQuery11Handler.class);
        registerOperationHandler(LdbcSnbBiQuery12.class, LdbcQuery12Handler.class);
        registerOperationHandler(LdbcSnbBiQuery13.class, LdbcQuery13Handler.class);
        registerOperationHandler(LdbcSnbBiQuery14.class, LdbcQuery14Handler.class);
        // Short Reads
        registerOperationHandler(LdbcShortQuery1PersonProfile.class, LdbcShortQuery1PersonProfileHandler.class);
        registerOperationHandler(LdbcShortQuery2PersonPosts.class, LdbcShortQuery2PersonPostsHandler.class);
        registerOperationHandler(LdbcShortQuery3PersonFriends.class, LdbcShortQuery3PersonFriendsHandler.class);
        registerOperationHandler(LdbcShortQuery4MessageContent.class, LdbcShortQuery4MessageContentHandler.class);
        registerOperationHandler(LdbcShortQuery5MessageCreator.class, LdbcShortQuery5MessageCreatorHandler.class);
        registerOperationHandler(LdbcShortQuery6MessageForum.class, LdbcShortQuery6MessageForumHandler.class);
        registerOperationHandler(LdbcShortQuery7MessageReplies.class, LdbcShortQuery7MessageRepliesHandler.class);
        // Updates
        registerOperationHandler(LdbcUpdate1AddPerson.class, LdbcUpdate1AddPersonHandler.class);
        registerOperationHandler(LdbcUpdate2AddPostLike.class, LdbcUpdate2AddPostLikeHandler.class);
        registerOperationHandler(LdbcUpdate3AddCommentLike.class, LdbcUpdate3AddCommentLikeHandler.class);
        registerOperationHandler(LdbcUpdate4AddForum.class, LdbcUpdate4AddForumHandler.class);
        registerOperationHandler(LdbcUpdate5AddForumMembership.class, LdbcUpdate5AddForumMembershipHandler.class);
        registerOperationHandler(LdbcUpdate6AddPost.class, LdbcUpdate6AddPostHandler.class);
        registerOperationHandler(LdbcUpdate7AddComment.class, LdbcUpdate7AddCommentHandler.class);
        registerOperationHandler(LdbcUpdate8AddFriendship.class, LdbcUpdate8AddFriendshipHandler.class);
    }

    @Override
    protected void onClose() throws IOException {
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return null;
    }

    private static void sleep(long sleepNs) {
        sleepFun.sleep(sleepNs);
    }

    /*
    LONG READS
     */

    private static final List<LdbcSnbBiQuery1Result> LDBC_QUERY_1_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read1Results();

    public static class LdbcQuery1Handler implements OperationHandler<LdbcSnbBiQuery1, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery1 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_1_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery2Result> LDBC_QUERY_2_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read2Results();

    public static class LdbcQuery2Handler implements OperationHandler<LdbcSnbBiQuery2, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery2 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_2_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery3Result> LDBC_QUERY_3_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read3Results();

    public static class LdbcQuery3Handler implements OperationHandler<LdbcSnbBiQuery3, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery3 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_3_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery4Result> LDBC_QUERY_4_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read4Results();

    public static class LdbcQuery4Handler implements OperationHandler<LdbcSnbBiQuery4, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery4 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_4_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery5Result> LDBC_QUERY_5_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read5Results();

    public static class LdbcQuery5Handler implements OperationHandler<LdbcSnbBiQuery5, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery5 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_5_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery6Result> LDBC_QUERY_6_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read6Results();

    public static class LdbcQuery6Handler implements OperationHandler<LdbcSnbBiQuery6, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery6 operation, DummyDbConnectionState dummyDbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_6_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery7Result> LDBC_QUERY_7_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read7Results();

    public static class LdbcQuery7Handler implements OperationHandler<LdbcSnbBiQuery7, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery7 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_7_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery8Result> LDBC_QUERY_8_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read8Results();

    public static class LdbcQuery8Handler implements OperationHandler<LdbcSnbBiQuery8, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery8 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_8_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery9Result> LDBC_QUERY_9_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read9Results();

    public static class LdbcQuery9Handler implements OperationHandler<LdbcSnbBiQuery9, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery9 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_9_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery10Result> LDBC_QUERY_10_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read10Results();

    public static class LdbcQuery10Handler implements OperationHandler<LdbcSnbBiQuery10, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery10 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_10_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery11Result> LDBC_QUERY_11_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read11Results();

    public static class LdbcQuery11Handler implements OperationHandler<LdbcSnbBiQuery11, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery11 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_11_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery12Result> LDBC_QUERY_12_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read12Results();

    public static class LdbcQuery12Handler implements OperationHandler<LdbcSnbBiQuery12, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery12 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_12_RESULTS, operation);
        }
    }

    private static final LdbcSnbBiQuery13Result
            LDBC_QUERY_13_RESULTS = DummyLdbcSnbInteractiveOperationResultSets.read13Results();

    public static class LdbcQuery13Handler implements OperationHandler<LdbcSnbBiQuery13, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery13 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_13_RESULTS, operation);
        }
    }

    private static final List<LdbcSnbBiQuery14Result> LDBC_QUERY_14_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .read14Results();

    public static class LdbcQuery14Handler implements OperationHandler<LdbcSnbBiQuery14, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcSnbBiQuery14 operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_QUERY_14_RESULTS, operation);
        }
    }

    /*
    SHORT READS
     */

    private static final LdbcShortQuery1PersonProfileResult LDBC_SHORT_QUERY_1_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short1Results();

    public static class LdbcShortQuery1PersonProfileHandler implements OperationHandler<LdbcShortQuery1PersonProfile, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery1PersonProfile operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_1_RESULTS, operation);
        }
    }

    private static final List<LdbcShortQuery2PersonPostsResult> LDBC_SHORT_QUERY_2_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short2Results();

    public static class LdbcShortQuery2PersonPostsHandler implements OperationHandler<LdbcShortQuery2PersonPosts, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery2PersonPosts operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_2_RESULTS, operation);
        }
    }

    private static final List<LdbcShortQuery3PersonFriendsResult> LDBC_SHORT_QUERY_3_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short3Results();

    public static class LdbcShortQuery3PersonFriendsHandler implements OperationHandler<LdbcShortQuery3PersonFriends, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery3PersonFriends operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_3_RESULTS, operation);
        }
    }

    private static final LdbcShortQuery4MessageContentResult LDBC_SHORT_QUERY_4_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short4Results();

    public static class LdbcShortQuery4MessageContentHandler implements OperationHandler<LdbcShortQuery4MessageContent, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery4MessageContent operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_4_RESULTS, operation);
        }
    }

    private static final LdbcShortQuery5MessageCreatorResult LDBC_SHORT_QUERY_5_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short5Results();

    public static class LdbcShortQuery5MessageCreatorHandler implements OperationHandler<LdbcShortQuery5MessageCreator, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery5MessageCreator operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_5_RESULTS, operation);
        }
    }

    private static final LdbcShortQuery6MessageForumResult LDBC_SHORT_QUERY_6_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short6Results();

    public static class LdbcShortQuery6MessageForumHandler implements OperationHandler<LdbcShortQuery6MessageForum, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery6MessageForum operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_6_RESULTS, operation);
        }
    }

    private static final List<LdbcShortQuery7MessageRepliesResult> LDBC_SHORT_QUERY_7_RESULTS = DummyLdbcSnbInteractiveOperationResultSets
            .short7Results();

    public static class LdbcShortQuery7MessageRepliesHandler implements OperationHandler<LdbcShortQuery7MessageReplies, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcShortQuery7MessageReplies operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LDBC_SHORT_QUERY_7_RESULTS, operation);
        }
    }

    /*
    UPDATES
     */

    public static class LdbcUpdate1AddPersonHandler implements OperationHandler<LdbcUpdate1AddPerson, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate1AddPerson operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate2AddPostLikeHandler implements OperationHandler<LdbcUpdate2AddPostLike, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate2AddPostLike operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate3AddCommentLikeHandler implements OperationHandler<LdbcUpdate3AddCommentLike, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate3AddCommentLike operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate4AddForumHandler implements OperationHandler<LdbcUpdate4AddForum, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate4AddForum operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate5AddForumMembershipHandler implements OperationHandler<LdbcUpdate5AddForumMembership, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate5AddForumMembership operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate6AddPostHandler implements OperationHandler<LdbcUpdate6AddPost, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate6AddPost operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate7AddCommentHandler implements OperationHandler<LdbcUpdate7AddComment, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate7AddComment operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }

    public static class LdbcUpdate8AddFriendshipHandler implements OperationHandler<LdbcUpdate8AddFriendship, DummyDbConnectionState> {
        @Override
        public void executeOperation(LdbcUpdate8AddFriendship operation, DummyDbConnectionState dbConnectionState, ResultReporter resultReporter) throws DbException {
            sleep(sleepDurationAsNano);
            resultReporter.report(0, LdbcNoResult.INSTANCE, operation);
        }
    }
}
