package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.util.List;

public class DummyLdbcSnbInteractiveOperationResultInstances {

    /*
    LONG READS
     */

    public static LdbcQuery1Result read1Result() {
        return new LdbcQuery1Result(1, "\u16a0", 3, 4, 5, "\u3055", "\u4e35", "\u0634", Lists.newArrayList("\u05e4"), Lists.newArrayList("10"), "11", Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("12", "13", "14")), Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("15", "16", "17")));
    }

    public static LdbcQuery2Result read2Result() {
        return new LdbcQuery2Result(1, "\u16a0", "\u3055", 4, "\u4e35", 6);
    }

    public static LdbcQuery3Result read3Result() {
        return new LdbcQuery3Result(1, "\u16a0", "\u3055", 4, 5, 6);
    }

    public static LdbcQuery4Result read4Result() {
        return new LdbcQuery4Result("\u16a0", 2);
    }

    public static LdbcQuery5Result read5Result() {
        return new LdbcQuery5Result("\u16a0", 2);
    }

    public static LdbcQuery6Result read6Result() {
        return new LdbcQuery6Result("\u16a0", 2);
    }

    public static LdbcQuery7Result read7Result() {
        return new LdbcQuery7Result(1, "\u16a0", "\u3055", 4, 5, "\u4e35", 7, false);
    }

    public static LdbcQuery8Result read8Result() {
        return new LdbcQuery8Result(1, "\u16a0", "\u3055", 4, 5, "\u4e35");
    }

    public static LdbcQuery9Result read9Result() {
        return new LdbcQuery9Result(1, "\u16a0", "\u3055", 4, "5", 6);
    }

    public static LdbcQuery10Result read10Result() {
        return new LdbcQuery10Result(1, "\u16a0", "\u3055", 4, "\u4e35", "\u0634");
    }

    public static LdbcQuery11Result read11Result() {
        return new LdbcQuery11Result(1, "\u16a0", "\u3055", "\u4e35", 5);
    }

    public static LdbcQuery12Result read12Result() {
        return new LdbcQuery12Result(1, "\u16a0", "\u3055", Lists.newArrayList("\u4e35"), 5);
    }

    public static LdbcQuery13Result read13Result() {
	return new LdbcQuery13Result(1, "\u16a0", "\u3055", 4, "\u4e35", 6);
    }

    public static LdbcQuery14Result read14Result() {
	return new LdbcQuery14Result("\u16a0", 2);
    }

    /*
    SHORT READS
     */

    public static LdbcShortQuery1PersonProfileResult short1Result() {
        return new LdbcShortQuery1PersonProfileResult("\u16a0", "\u3055", 1, "\u4e35", "\u0634", 2, "a", 3);
    }

    public static LdbcShortQuery2PersonPostsResult short2Result() {
        return new LdbcShortQuery2PersonPostsResult(1, "\u0634", 2, 3, 4, "a", "b");
    }

    public static LdbcShortQuery3PersonFriendsResult short3Result() {
        return new LdbcShortQuery3PersonFriendsResult(1, "\u0634", "\u3055", 2);
    }

    public static LdbcShortQuery4MessageContentResult short4Result() {
        return new LdbcShortQuery4MessageContentResult("\u16a0", 1);
    }

    public static LdbcShortQuery5MessageCreatorResult short5Result() {
        return new LdbcShortQuery5MessageCreatorResult(1, "\u0634", "\u3055");
    }

    public static LdbcShortQuery6MessageForumResult short6Result() {
        return new LdbcShortQuery6MessageForumResult(1, "\u0634", 2, "\u4e35", "\u0634");
    }

    public static LdbcShortQuery7MessageRepliesResult short7Result() {
        return new LdbcShortQuery7MessageRepliesResult(1, "\u0634", 2, 2, "\u4e35", "\u3055", true);
    }
}
