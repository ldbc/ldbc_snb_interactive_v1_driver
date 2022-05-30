package org.ldbcouncil.snb.driver.workloads.interactive.db;

import com.google.common.collect.Lists;
import org.ldbcouncil.snb.driver.workloads.interactive.*;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery10Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery11Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery12Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery13Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery14Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery1Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery2Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery3Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery4Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery5Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery6Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery7Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery8Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcQuery9Result;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery1PersonProfileResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery2PersonPostsResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery3PersonFriendsResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery4MessageContentResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery5MessageCreatorResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery6MessageForumResult;
import org.ldbcouncil.snb.driver.workloads.interactive.queries.LdbcShortQuery7MessageRepliesResult;

import java.util.Arrays;

public class DummyLdbcSnbInteractiveOperationResultInstances {

    /*
    LONG READS
     */

    public static LdbcQuery1Result read1Result() {
        return new LdbcQuery1Result(1, "\u16a0", 3, 4, 5, "\u3055", "\u4e35", "\u0634",
        Lists.newArrayList("\u05e4"),
        Lists.newArrayList("10"),
        "11",
        Arrays.asList(new LdbcQuery1Result.Organization("someCompany", 13, "14")),
        Arrays.asList(new LdbcQuery1Result.Organization("someUniversity", 16, "17")));
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
        return new LdbcQuery13Result(1);
    }

    public static LdbcQuery14Result read14Result() {
        return new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 3);
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
