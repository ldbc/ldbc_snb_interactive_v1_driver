package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPersonResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiatorsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormalsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCountsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteractionResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummaryResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21ZombiesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialogResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinationsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopicResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolutionResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPostersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePostersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsersResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopicsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForumsResult;

import java.util.List;

public class DummyLdbcSnbBiOperationResultSets
{

    /*
    LONG READS
     */

    public static List<LdbcSnbBiQuery1PostingSummaryResult> read1Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result(),
                DummyLdbcSnbBiOperationResultInstances.read1Result()
        );
    }

    public static List<LdbcSnbBiQuery2TopTagsResult> read2Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result(),
                DummyLdbcSnbBiOperationResultInstances.read2Result()
        );
    }

    public static List<LdbcSnbBiQuery3TagEvolutionResult> read3Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result(),
                DummyLdbcSnbBiOperationResultInstances.read3Result()
        );
    }

    public static List<LdbcSnbBiQuery4PopularCountryTopicsResult> read4Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result(),
                DummyLdbcSnbBiOperationResultInstances.read4Result()
        );
    }

    public static List<LdbcSnbBiQuery5TopCountryPostersResult> read5Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result(),
                DummyLdbcSnbBiOperationResultInstances.read5Result()
        );
    }

    public static List<LdbcSnbBiQuery6ActivePostersResult> read6Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result(),
                DummyLdbcSnbBiOperationResultInstances.read6Result()
        );
    }

    public static List<LdbcSnbBiQuery7AuthoritativeUsersResult> read7Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result(),
                DummyLdbcSnbBiOperationResultInstances.read7Result()
        );
    }

    public static List<LdbcSnbBiQuery8RelatedTopicsResult> read8Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result(),
                DummyLdbcSnbBiOperationResultInstances.read8Result()
        );
    }

    public static List<LdbcSnbBiQuery9RelatedForumsResult> read9Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result(),
                DummyLdbcSnbBiOperationResultInstances.read9Result()
        );
    }

    public static List<LdbcSnbBiQuery10TagPersonResult> read10Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result(),
                DummyLdbcSnbBiOperationResultInstances.read10Result()
        );
    }

    public static List<LdbcSnbBiQuery11UnrelatedRepliesResult> read11Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result(),
                DummyLdbcSnbBiOperationResultInstances.read11Result()
        );
    }

    public static List<LdbcSnbBiQuery12TrendingPostsResult> read12Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result(),
                DummyLdbcSnbBiOperationResultInstances.read12Result()
        );
    }

    public static List<LdbcSnbBiQuery13PopularMonthlyTagsResult> read13Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read13Result()
        );
    }

    public static List<LdbcSnbBiQuery14TopThreadInitiatorsResult> read14Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result(),
                DummyLdbcSnbBiOperationResultInstances.read14Result()
        );
    }

    public static List<LdbcSnbBiQuery15SocialNormalsResult> read15Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read15Result(),
                DummyLdbcSnbBiOperationResultInstances.read15Result(),
                DummyLdbcSnbBiOperationResultInstances.read15Result(),
                DummyLdbcSnbBiOperationResultInstances.read15Result(),
                DummyLdbcSnbBiOperationResultInstances.read15Result()
        );
    }

    public static List<LdbcSnbBiQuery16ExpertsInSocialCircleResult> read16Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result(),
                DummyLdbcSnbBiOperationResultInstances.read16Result()
        );
    }

    // read17Results not needed, because it only returns a single result object, not a collection

    public static List<LdbcSnbBiQuery18PersonPostCountsResult> read18Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read18Result(),
                DummyLdbcSnbBiOperationResultInstances.read18Result(),
                DummyLdbcSnbBiOperationResultInstances.read18Result(),
                DummyLdbcSnbBiOperationResultInstances.read18Result()
        );
    }

    public static List<LdbcSnbBiQuery19StrangerInteractionResult> read19Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result(),
                DummyLdbcSnbBiOperationResultInstances.read19Result()
        );
    }

    public static List<LdbcSnbBiQuery20HighLevelTopicsResult> read20Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result(),
                DummyLdbcSnbBiOperationResultInstances.read20Result()
        );
    }

    public static List<LdbcSnbBiQuery21ZombiesResult> read21Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read21Result(),
                DummyLdbcSnbBiOperationResultInstances.read21Result()
        );
    }

    public static List<LdbcSnbBiQuery22InternationalDialogResult> read22Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read22Result(),
                DummyLdbcSnbBiOperationResultInstances.read22Result(),
                DummyLdbcSnbBiOperationResultInstances.read22Result(),
                DummyLdbcSnbBiOperationResultInstances.read22Result()
        );
    }

    public static List<LdbcSnbBiQuery23HolidayDestinationsResult> read23Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read23Result()
        );
    }

    public static List<LdbcSnbBiQuery24MessagesByTopicResult> read24Results()
    {
        return Lists.newArrayList(
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result(),
                DummyLdbcSnbBiOperationResultInstances.read24Result()
        );
    }
}
