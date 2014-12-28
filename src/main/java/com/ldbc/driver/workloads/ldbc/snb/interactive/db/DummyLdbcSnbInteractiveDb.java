package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.ldbc.driver.*;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.io.IOException;
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
        void sleep(long sleepNs);
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
                sleepType = SleepType.valueOf(properties.get(SLEEP_TYPE_ARG));
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
        registerOperationHandler(LdbcQuery1.class, LdbcQuery1Handler.class);
        registerOperationHandler(LdbcQuery2.class, LdbcQuery2Handler.class);
        registerOperationHandler(LdbcQuery3.class, LdbcQuery3Handler.class);
        registerOperationHandler(LdbcQuery4.class, LdbcQuery4Handler.class);
        registerOperationHandler(LdbcQuery5.class, LdbcQuery5Handler.class);
        registerOperationHandler(LdbcQuery6.class, LdbcQuery6Handler.class);
        registerOperationHandler(LdbcQuery7.class, LdbcQuery7Handler.class);
        registerOperationHandler(LdbcQuery8.class, LdbcQuery8Handler.class);
        registerOperationHandler(LdbcQuery9.class, LdbcQuery9Handler.class);
        registerOperationHandler(LdbcQuery10.class, LdbcQuery10Handler.class);
        registerOperationHandler(LdbcQuery11.class, LdbcQuery11Handler.class);
        registerOperationHandler(LdbcQuery12.class, LdbcQuery12Handler.class);
        registerOperationHandler(LdbcQuery13.class, LdbcQuery13Handler.class);
        registerOperationHandler(LdbcQuery14.class, LdbcQuery14Handler.class);
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

    public static class LdbcQuery1Handler extends OperationHandler<LdbcQuery1, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery1 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read1Results());
        }
    }

    public static class LdbcQuery2Handler extends OperationHandler<LdbcQuery2, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery2 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read2Results());
        }
    }

    public static class LdbcQuery3Handler extends OperationHandler<LdbcQuery3, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery3 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read3Results());
        }
    }

    public static class LdbcQuery4Handler extends OperationHandler<LdbcQuery4, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery4 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read4Results());
        }
    }

    public static class LdbcQuery5Handler extends OperationHandler<LdbcQuery5, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery5 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read5Results());
        }
    }

    public static class LdbcQuery6Handler extends OperationHandler<LdbcQuery6, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery6 operation, DummyDbConnectionState dummyDbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read6Results());
        }
    }

    public static class LdbcQuery7Handler extends OperationHandler<LdbcQuery7, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery7 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read7Results());
        }
    }

    public static class LdbcQuery8Handler extends OperationHandler<LdbcQuery8, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery8 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read8Results());
        }
    }

    public static class LdbcQuery9Handler extends OperationHandler<LdbcQuery9, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery9 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read9Results());
        }
    }

    public static class LdbcQuery10Handler extends OperationHandler<LdbcQuery10, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery10 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read10Results());
        }
    }

    public static class LdbcQuery11Handler extends OperationHandler<LdbcQuery11, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery11 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read11Results());
        }
    }

    public static class LdbcQuery12Handler extends OperationHandler<LdbcQuery12, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery12 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read12Results());
        }
    }

    public static class LdbcQuery13Handler extends OperationHandler<LdbcQuery13, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery13 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read13Results());
        }
    }

    public static class LdbcQuery14Handler extends OperationHandler<LdbcQuery14, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcQuery14 operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.read14Results());
        }
    }

    /*
    SHORT READS
     */

    public static class LdbcShortQuery1PersonProfileHandler extends OperationHandler<LdbcShortQuery1PersonProfile, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery1PersonProfile operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short1Results());
        }
    }

    public static class LdbcShortQuery2PersonPostsHandler extends OperationHandler<LdbcShortQuery2PersonPosts, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery2PersonPosts operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short2Results());
        }
    }

    public static class LdbcShortQuery3PersonFriendsHandler extends OperationHandler<LdbcShortQuery3PersonFriends, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery3PersonFriends operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short3Results());
        }
    }

    public static class LdbcShortQuery4MessageContentHandler extends OperationHandler<LdbcShortQuery4MessageContent, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery4MessageContent operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short4Results());
        }
    }

    public static class LdbcShortQuery5MessageCreatorHandler extends OperationHandler<LdbcShortQuery5MessageCreator, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery5MessageCreator operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short5Results());
        }
    }

    public static class LdbcShortQuery6MessageForumHandler extends OperationHandler<LdbcShortQuery6MessageForum, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery6MessageForum operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short6Results());
        }
    }

    public static class LdbcShortQuery7MessageRepliesHandler extends OperationHandler<LdbcShortQuery7MessageReplies, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcShortQuery7MessageReplies operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, DummyLdbcSnbInteractiveOperationResultSets.short7Results());
        }
    }

    /*
    UPDATES
     */

    public static class LdbcUpdate1AddPersonHandler extends OperationHandler<LdbcUpdate1AddPerson, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate1AddPerson operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate2AddPostLikeHandler extends OperationHandler<LdbcUpdate2AddPostLike, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate2AddPostLike operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate3AddCommentLikeHandler extends OperationHandler<LdbcUpdate3AddCommentLike, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate3AddCommentLike operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate4AddForumHandler extends OperationHandler<LdbcUpdate4AddForum, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate4AddForum operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate5AddForumMembershipHandler extends OperationHandler<LdbcUpdate5AddForumMembership, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate5AddForumMembership operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate6AddPostHandler extends OperationHandler<LdbcUpdate6AddPost, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate6AddPost operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate7AddCommentHandler extends OperationHandler<LdbcUpdate7AddComment, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate7AddComment operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }

    public static class LdbcUpdate8AddFriendshipHandler extends OperationHandler<LdbcUpdate8AddFriendship, DummyDbConnectionState> {
        @Override
        public OperationResultReport executeOperation(LdbcUpdate8AddFriendship operation, DummyDbConnectionState dbConnectionState) throws DbException {
            sleep(sleepDurationAsNano);
            return operation.buildResult(0, null);
        }
    }
}
