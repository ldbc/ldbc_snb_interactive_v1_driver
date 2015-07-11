package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1Result;

import java.util.List;

public class DummyLdbcSnbInteractiveOperationResultInstances {

    /*
    LONG READS
     */

    public static LdbcSnbBiQuery1Result read1Result() {
        return new LdbcSnbBiQuery1Result(1, "ᚠ", 3, 4, 5, "さ", "丵", "ش", Lists.newArrayList("פ"), Lists.newArrayList("10"), "11", Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("12", "13", "14")), Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("15", "16", "17")));
    }

    public static LdbcSnbBiQuery2Result read2Result() {
        return new LdbcSnbBiQuery2Result(1, "ᚠ", "さ", 4, "丵", 6);
    }

    public static LdbcSnbBiQuery3Result read3Result() {
        return new LdbcSnbBiQuery3Result(1, "ᚠ", "さ", 4, 5, 6);
    }

    public static LdbcSnbBiQuery4Result read4Result() {
        return new LdbcSnbBiQuery4Result("ᚠ", 2);
    }

    public static LdbcSnbBiQuery5Result read5Result() {
        return new LdbcSnbBiQuery5Result("ᚠ", 2);
    }

    public static LdbcSnbBiQuery6Result read6Result() {
        return new LdbcSnbBiQuery6Result("ᚠ", 2);
    }

    public static LdbcSnbBiQuery7Result read7Result() {
        return new LdbcSnbBiQuery7Result(1, "ᚠ", "さ", 4, 5, "丵", 7, false);
    }

    public static LdbcSnbBiQuery8Result read8Result() {
        return new LdbcSnbBiQuery8Result(1, "ᚠ", "さ", 4, 5, "丵");
    }

    public static LdbcSnbBiQuery9Result read9Result() {
        return new LdbcSnbBiQuery9Result(1, "ᚠ", "さ", 4, "5", 6);
    }

    public static LdbcSnbBiQuery10Result read10Result() {
        return new LdbcSnbBiQuery10Result(1, "ᚠ", "さ", 4, "丵", "ش");
    }

    public static LdbcSnbBiQuery11Result read11Result() {
        return new LdbcSnbBiQuery11Result(1, "ᚠ", "さ", "丵", 5);
    }

    public static LdbcSnbBiQuery12Result read12Result() {
        return new LdbcSnbBiQuery12Result(1, "ᚠ", "さ", Lists.newArrayList("丵"), 5);
    }

    public static LdbcSnbBiQuery13Result read13Result() {
        return new LdbcSnbBiQuery13Result(1);
    }

    public static LdbcSnbBiQuery14Result read14Result() {
        return new LdbcSnbBiQuery14Result(Lists.newArrayList(1l, 2l), 3);
    }

    /*
    SHORT READS
     */

    public static LdbcShortQuery1PersonProfileResult short1Result() {
        return new LdbcShortQuery1PersonProfileResult("ᚠ", "さ", 1, "丵", "ش", 2, "a", 3);
    }

    public static LdbcShortQuery2PersonPostsResult short2Result() {
        return new LdbcShortQuery2PersonPostsResult(1, "ش", 2, 3, 4, "a", "b");
    }

    public static LdbcShortQuery3PersonFriendsResult short3Result() {
        return new LdbcShortQuery3PersonFriendsResult(1, "ش", "さ", 2);
    }

    public static LdbcShortQuery4MessageContentResult short4Result() {
        return new LdbcShortQuery4MessageContentResult("ᚠ", 1);
    }

    public static LdbcShortQuery5MessageCreatorResult short5Result() {
        return new LdbcShortQuery5MessageCreatorResult(1, "ش", "さ");
    }

    public static LdbcShortQuery6MessageForumResult short6Result() {
        return new LdbcShortQuery6MessageForumResult(1, "ش", 2, "丵", "ش");
    }

    public static LdbcShortQuery7MessageRepliesResult short7Result() {
        return new LdbcShortQuery7MessageRepliesResult(1, "ش", 2, 2, "丵", "さ", true);
    }
}
