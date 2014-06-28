package com.ldbc.driver.workloads.ldbc.snb.interactive.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;

public class DummyLdbcSnbInteractiveOperationResultInstances {
    public static LdbcQuery1Result read1Result() {
        return new LdbcQuery1Result(1, "2", 3, 4, 5, "6", "7", "8", Lists.newArrayList("9"), Lists.newArrayList("10"), "11", Lists.newArrayList("12"), Lists.newArrayList("13"));
    }

    public static LdbcQuery2Result read2Result() {
        return new LdbcQuery2Result(1, "2", "3", 4, "5", 6);
    }

    public static LdbcQuery3Result read3Result() {
        return new LdbcQuery3Result(1, "2", "3", 4, 5, 6);
    }

    public static LdbcQuery4Result read4Result() {
        return new LdbcQuery4Result("1", 2);
    }

    public static LdbcQuery5Result read5Result() {
        return new LdbcQuery5Result("1", 2);
    }

    public static LdbcQuery6Result read6Result() {
        return new LdbcQuery6Result("1", 2);
    }

    public static LdbcQuery7Result read7Result() {
        return new LdbcQuery7Result(1, "2", "3", 4, 5, "6", 7, false);
    }

    public static LdbcQuery8Result read8Result() {
        return new LdbcQuery8Result(1, "2", "3", 4, 5, "6");
    }

    public static LdbcQuery9Result read9Result() {
        return new LdbcQuery9Result(1, "2", "3", 4, "5", 6);
    }

    public static LdbcQuery10Result read10Result() {
        return new LdbcQuery10Result(1, "2", "3", 4, "5", "6");
    }

    public static LdbcQuery11Result read11Result() {
        return new LdbcQuery11Result(1, "2", "3", "4", 5);
    }

    public static LdbcQuery12Result read12Result() {
        return new LdbcQuery12Result(1, "2", "3", Lists.newArrayList("4"), 5);
    }

    public static LdbcQuery13Result read13Result() {
        return new LdbcQuery13Result(1);
    }

    public static LdbcQuery14Result read14Result() {
        return new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 3);
    }
}
