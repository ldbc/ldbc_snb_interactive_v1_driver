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

    public static LdbcSnbBiQuery10TagPersonResult read10Result()
    {
        return new LdbcSnbBiQuery10TagPersonResult( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery14TopThreadInitiatorsResult read14Result()
    {
        return new LdbcSnbBiQuery14TopThreadInitiatorsResult( 1, "2", "3", 4, 5 );
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

    public static LdbcSnbBiQuery21ZombiesResult read21Result()
    {
        return new LdbcSnbBiQuery21ZombiesResult( 1, 2, 3, 4.5 );
    }

    public static LdbcSnbBiQuery22InternationalDialogResult read22Result()
    {
        return new LdbcSnbBiQuery22InternationalDialogResult( 1, 2,  "Paris", 3 );
    }

    public static LdbcSnbBiQuery25WeightedPathsResult read25Result()
    {
        return new LdbcSnbBiQuery25WeightedPathsResult( Lists.newArrayList(1L, 2L), 0.5);
    }
}
