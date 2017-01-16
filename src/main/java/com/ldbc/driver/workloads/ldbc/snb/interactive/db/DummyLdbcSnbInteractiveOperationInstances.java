package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.util.Date;

public class DummyLdbcSnbInteractiveOperationInstances {

    /*
    LONG READS
     */

    public static LdbcQuery1 read1() {
        return new LdbcQuery1(1, "3", 4);
    }

    public static LdbcQuery2 read2() {
        return new LdbcQuery2(1, new Date(3), 4);
    }

    public static LdbcQuery3 read3() {
        return new LdbcQuery3(1, "3", "4", new Date(5), 6, 7);
    }

    public static LdbcQuery4 read4() {
        return new LdbcQuery4(1, new Date(3), 4, 5);
    }

    public static LdbcQuery5 read5() {
        return new LdbcQuery5(1, new Date(3), 4);
    }

    public static LdbcQuery6 read6() {
        return new LdbcQuery6(1, "3", 4);
    }

    public static LdbcQuery7 read7() {
        return new LdbcQuery7(1, 3);
    }

    public static LdbcQuery8 read8() {
        return new LdbcQuery8(1, 3);
    }

    public static LdbcQuery9 read9() {
        return new LdbcQuery9(1, new Date(3), 4);
    }

    public static LdbcQuery10 read10() {
        return new LdbcQuery10(1, 3, 4);
    }

    public static LdbcQuery11 read11() {
        return new LdbcQuery11(1, "3", 4, 5);
    }

    public static LdbcQuery12 read12() {
        return new LdbcQuery12(1, "3", 4);
    }

    public static LdbcQuery13 read13() {
        return new LdbcQuery13(1, new Date(3));
    }

    public static LdbcQuery14 read14() {
        return new LdbcQuery14(1, new Date(3));
    }

    /*
    SHORT READS
     */

    public static LdbcShortQuery1PersonProfile short1() {
        return new LdbcShortQuery1PersonProfile(1);
    }

    public static LdbcShortQuery2PersonPosts short2() {
        return new LdbcShortQuery2PersonPosts(2, 3);
    }

    public static LdbcShortQuery3PersonFriends short3() {
        return new LdbcShortQuery3PersonFriends(3);
    }

    public static LdbcShortQuery4MessageContent short4() {
        return new LdbcShortQuery4MessageContent(4);
    }

    public static LdbcShortQuery5MessageCreator short5() {
        return new LdbcShortQuery5MessageCreator(5);
    }

    public static LdbcShortQuery6MessageForum short6() {
        return new LdbcShortQuery6MessageForum(6);
    }

    public static LdbcShortQuery7MessageReplies short7() {
        return new LdbcShortQuery7MessageReplies(7);
    }

    /*
    UPDATES
     */

    public static LdbcUpdate1AddPerson write1() {
        return new LdbcUpdate1AddPerson(
                1, "2", "3", "4", new Date(5), new Date(6), "7", "8", 9, Lists.newArrayList("10", "11"), Lists.<String>newArrayList(), Lists.newArrayList(13l),
                Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(14, 15), new LdbcUpdate1AddPerson.Organization(16, 17)),
                Lists.<LdbcUpdate1AddPerson.Organization>newArrayList());
    }

    public static LdbcUpdate2AddPostLike write2() {
        return new LdbcUpdate2AddPostLike(1, 2, new Date(3));
    }

    public static LdbcUpdate3AddCommentLike write3() {
        return new LdbcUpdate3AddCommentLike(1, 2, new Date(3));
    }

    public static LdbcUpdate4AddForum write4() {
        return new LdbcUpdate4AddForum(1, "2", new Date(3), 4, Lists.newArrayList(5l, 6l));
    }

    public static LdbcUpdate5AddForumMembership write5() {
        return new LdbcUpdate5AddForumMembership(1, 2, new Date(3));
    }

    public static LdbcUpdate6AddPost write6() {
        return new LdbcUpdate6AddPost(1, "2", new Date(3), "4", "5", "6", "7", 8, 9, 10, 11, Lists.newArrayList(12l), Lists.newArrayList(13l), null, "14");
    }

    public static LdbcUpdate7AddComment write7() {
        return new LdbcUpdate7AddComment(1, new Date(2), "3", "4", "5", 6, 7, 8, 9, 10, Lists.newArrayList(11l, 12l), Lists.newArrayList(13l, 14l), null, "15", "16");
    }

    public static LdbcUpdate8AddFriendship write8() {
        return new LdbcUpdate8AddFriendship(1, 2, new Date(3));
    }
}
