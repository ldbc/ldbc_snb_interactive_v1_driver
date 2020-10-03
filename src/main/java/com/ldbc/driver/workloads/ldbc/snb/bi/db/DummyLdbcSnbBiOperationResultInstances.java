package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.bi.*;

public class DummyLdbcSnbBiOperationResultInstances
{

    /*
    LONG READS
     */

    public static LdbcSnbBiQuery1PostingSummaryResult read1Result()
    {
        return new LdbcSnbBiQuery1PostingSummaryResult( 1, false, 2, 3, 4, 5, 6.0f );
    }

    public static LdbcSnbBiQuery2TagEvolutionResult read2Result()
    {
        return new LdbcSnbBiQuery2TagEvolutionResult( "1", 2, 3, 4 );
    }

    public static LdbcSnbBiQuery3PopularCountryTopicsResult read3Result()
    {
        return new LdbcSnbBiQuery3PopularCountryTopicsResult( 1, "\u16a0", Long.MAX_VALUE, Long.MIN_VALUE, 2 );
    }

    public static LdbcSnbBiQuery4TopCountryPostersResult read4Result()
    {
        return new LdbcSnbBiQuery4TopCountryPostersResult( 1, "\u0634", "\u16a0", 2, 3 );
    }

    public static LdbcSnbBiQuery5ActivePostersResult read5Result()
    {
        return new LdbcSnbBiQuery5ActivePostersResult( 1, 2, 3, 4, 5 );
    }

    public static LdbcSnbBiQuery6AuthoritativeUsersResult read6Result()
    {
        return new LdbcSnbBiQuery6AuthoritativeUsersResult( 1, 2 );
    }

    public static LdbcSnbBiQuery7RelatedTopicsResult read7Result()
    {
        return new LdbcSnbBiQuery7RelatedTopicsResult( "\u4e35", 2 );
    }

    public static LdbcSnbBiQuery8TagPersonResult read8Result()
    {
        return new LdbcSnbBiQuery8TagPersonResult( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery9TopThreadInitiatorsResult read9Result()
    {
        return new LdbcSnbBiQuery9TopThreadInitiatorsResult( 1, "2", "3", 4, 5 );
    }

    public static LdbcSnbBiQuery10ExpertsInSocialCircleResult read10Result()
    {
        return new LdbcSnbBiQuery10ExpertsInSocialCircleResult( 1, "2", 3 );
    }

    public static LdbcSnbBiQuery11FriendshipTrianglesResult read11Result()
    {
        return new LdbcSnbBiQuery11FriendshipTrianglesResult( 1 );
    }

    public static LdbcSnbBiQuery12PersonPostCountsResult read12Result()
    {
        return new LdbcSnbBiQuery12PersonPostCountsResult( 1, 2 );
    }

    public static LdbcSnbBiQuery13ZombiesResult read13Result()
    {
        return new LdbcSnbBiQuery13ZombiesResult( 1, 2, 3, 4.5 );
    }

    public static LdbcSnbBiQuery14InternationalDialogResult read14Result()
    {
        return new LdbcSnbBiQuery14InternationalDialogResult( 1, 2,  "Paris", 3 );
    }

    public static LdbcSnbBiQuery15WeightedPathsResult read15Result()
    {
        return new LdbcSnbBiQuery15WeightedPathsResult( Lists.newArrayList(1L, 2L), 0.5);
    }
}
