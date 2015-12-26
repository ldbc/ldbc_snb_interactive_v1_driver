package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPersonResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedRepliesResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPostsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTagsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiatorsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormalsResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircleResult;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTrianglesResult;
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

public class DummyLdbcSnbBiOperationResultInstances
{

    /*
    LONG READS
     */

    public static LdbcSnbBiQuery1PostingSummaryResult read1Result()
    {
        return new LdbcSnbBiQuery1PostingSummaryResult( 1, false, 2, 3, 4, 5, 6.0f );
    }

    public static LdbcSnbBiQuery2TopTagsResult read2Result()
    {
        return new LdbcSnbBiQuery2TopTagsResult( "\u16a0", 1, "\u3055", 2, "\u4e35", 3 );
    }

    public static LdbcSnbBiQuery3TagEvolutionResult read3Result()
    {
        return new LdbcSnbBiQuery3TagEvolutionResult( "1", 2, 3, 4 );
    }

    public static LdbcSnbBiQuery4PopularCountryTopicsResult read4Result()
    {
        return new LdbcSnbBiQuery4PopularCountryTopicsResult( 1, "\u16a0", Long.MAX_VALUE, Long.MIN_VALUE, 2 );
    }

    public static LdbcSnbBiQuery5TopCountryPostersResult read5Result()
    {
        return new LdbcSnbBiQuery5TopCountryPostersResult( 1, "\u0634", "\u16a0", 2, 3 );
    }

    public static LdbcSnbBiQuery6ActivePostersResult read6Result()
    {
        return new LdbcSnbBiQuery6ActivePostersResult( 1, 2, 3, 4, 5 );
    }

    public static LdbcSnbBiQuery7AuthoritativeUsersResult read7Result()
    {
        return new LdbcSnbBiQuery7AuthoritativeUsersResult( 1, 2 );
    }

    public static LdbcSnbBiQuery8RelatedTopicsResult read8Result()
    {
        return new LdbcSnbBiQuery8RelatedTopicsResult( "\u4e35", 2 );
    }

    public static LdbcSnbBiQuery9RelatedForumsResult read9Result()
    {
        return new LdbcSnbBiQuery9RelatedForumsResult( Long.MAX_VALUE, 1, 2 );
    }

    public static LdbcSnbBiQuery10TagPersonResult read10Result()
    {
        return new LdbcSnbBiQuery10TagPersonResult( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery11UnrelatedRepliesResult read11Result()
    {
        return new LdbcSnbBiQuery11UnrelatedRepliesResult( 1, "\u16a0", 2, 3 );
    }

    public static LdbcSnbBiQuery12TrendingPostsResult read12Result()
    {
        return new LdbcSnbBiQuery12TrendingPostsResult( 1, "\u16a0", "\u3055", 2, 3 );
    }

    public static LdbcSnbBiQuery13PopularMonthlyTagsResult read13Result()
    {
        return new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                1,
                2,
                Lists.newArrayList(
                        new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "tag1", 1 ),
                        new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "tag2", 2 ),
                        new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "tag3", 3 )
                )
        );
    }

    public static LdbcSnbBiQuery14TopThreadInitiatorsResult read14Result()
    {
        return new LdbcSnbBiQuery14TopThreadInitiatorsResult( 1, "2", "3", 4, 5 );
    }

    public static LdbcSnbBiQuery15SocialNormalsResult read15Result()
    {
        return new LdbcSnbBiQuery15SocialNormalsResult( 1, 2 );
    }

    public static LdbcSnbBiQuery16ExpertsInSocialCircleResult read16Result()
    {
        return new LdbcSnbBiQuery16ExpertsInSocialCircleResult( 1, "2", 3 );
    }

    public static LdbcSnbBiQuery17FriendshipTrianglesResult read17Result()
    {
        return new LdbcSnbBiQuery17FriendshipTrianglesResult( 1 );
    }

    public static LdbcSnbBiQuery18PersonPostCountsResult read18Result()
    {
        return new LdbcSnbBiQuery18PersonPostCountsResult( 1, 2 );
    }

    public static LdbcSnbBiQuery19StrangerInteractionResult read19Result()
    {
        return new LdbcSnbBiQuery19StrangerInteractionResult( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery20HighLevelTopicsResult read20Result()
    {
        return new LdbcSnbBiQuery20HighLevelTopicsResult( "\u16a0", 1 );
    }

    public static LdbcSnbBiQuery21ZombiesResult read21Result()
    {
        return new LdbcSnbBiQuery21ZombiesResult( 1, 2, 3, 4.5 );
    }

    public static LdbcSnbBiQuery22InternationalDialogResult read22Result()
    {
        return new LdbcSnbBiQuery22InternationalDialogResult( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery23HolidayDestinationsResult read23Result()
    {
        return new LdbcSnbBiQuery23HolidayDestinationsResult( "\u3055", 2, 3 );
    }

    public static LdbcSnbBiQuery24MessagesByTopicResult read24Result()
    {
        return new LdbcSnbBiQuery24MessagesByTopicResult( 1, 2, 3, 4, "5" );
    }
}
