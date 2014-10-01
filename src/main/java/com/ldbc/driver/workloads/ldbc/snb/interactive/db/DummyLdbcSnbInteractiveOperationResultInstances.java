package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

import java.util.List;

public class DummyLdbcSnbInteractiveOperationResultInstances {
    public static LdbcQuery1Result read1Result() {
        return new LdbcQuery1Result(1, "ᚠ", 3, 4, 5, "さ", "丵", "ش", Lists.newArrayList("פ"), Lists.newArrayList("10"), "11", Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("12", "13", "14")), Lists.<List<Object>>newArrayList(Lists.<Object>newArrayList("15", "16", "17")));
    }

    public static LdbcQuery2Result read2Result() {
        return new LdbcQuery2Result(1, "ᚠ", "さ", 4, "丵", 6);
    }

    public static LdbcQuery3Result read3Result() {
        return new LdbcQuery3Result(1, "ᚠ", "さ", 4, 5, 6);
    }

    public static LdbcQuery4Result read4Result() {
        return new LdbcQuery4Result("ᚠ", 2);
    }

    public static LdbcQuery5Result read5Result() {
        return new LdbcQuery5Result("ᚠ", 2);
    }

    public static LdbcQuery6Result read6Result() {
        return new LdbcQuery6Result("ᚠ", 2);
    }

    public static LdbcQuery7Result read7Result() {
        return new LdbcQuery7Result(1, "ᚠ", "さ", 4, 5, "丵", 7, false);
    }

    public static LdbcQuery8Result read8Result() {
        return new LdbcQuery8Result(1, "ᚠ", "さ", 4, 5, "丵");
    }

    public static LdbcQuery9Result read9Result() {
        return new LdbcQuery9Result(1, "ᚠ", "さ", 4, "5", 6);
    }

    public static LdbcQuery10Result read10Result() {
        return new LdbcQuery10Result(1, "ᚠ", "さ", 4, "丵", "ش");
    }

    public static LdbcQuery11Result read11Result() {
        return new LdbcQuery11Result(1, "ᚠ", "さ", "丵", 5);
    }

    public static LdbcQuery12Result read12Result() {
        return new LdbcQuery12Result(1, "ᚠ", "さ", Lists.newArrayList("丵"), 5);
    }

    public static LdbcQuery13Result read13Result() {
        return new LdbcQuery13Result(1);
    }

    public static LdbcQuery14Result read14Result() {
        return new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 3);
    }
}
