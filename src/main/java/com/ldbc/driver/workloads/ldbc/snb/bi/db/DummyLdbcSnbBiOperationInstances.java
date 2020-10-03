package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.bi.*;

public class DummyLdbcSnbBiOperationInstances
{

    /*
    LONG READS
     */

    public static LdbcSnbBiQuery1PostingSummary read1()
    {
        return new LdbcSnbBiQuery1PostingSummary( 1 );
    }

    public static LdbcSnbBiQuery3TagEvolution read3()
    {
        return new LdbcSnbBiQuery3TagEvolution( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery4PopularCountryTopics read4()
    {
        return new LdbcSnbBiQuery4PopularCountryTopics( "tagClass", "1", 2 );
    }

    public static LdbcSnbBiQuery5TopCountryPosters read5()
    {
        return new LdbcSnbBiQuery5TopCountryPosters( "\u16a0", 1 );
    }

    public static LdbcSnbBiQuery6ActivePosters read6()
    {
        return new LdbcSnbBiQuery6ActivePosters( "\u3055", 1 );
    }

    public static LdbcSnbBiQuery7AuthoritativeUsers read7()
    {
        return new LdbcSnbBiQuery7AuthoritativeUsers( "\u4e35", 1 );
    }

    public static LdbcSnbBiQuery8RelatedTopics read8()
    {
        return new LdbcSnbBiQuery8RelatedTopics( "\u0634", 1 );
    }

    public static LdbcSnbBiQuery10TagPerson read10()
    {
        return new LdbcSnbBiQuery10TagPerson( "\u3055", 1, 1 );
    }

    public static LdbcSnbBiQuery14TopThreadInitiators read14()
    {
        return new LdbcSnbBiQuery14TopThreadInitiators( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery16ExpertsInSocialCircle read16()
    {
        return new LdbcSnbBiQuery16ExpertsInSocialCircle( 0, "1", "2", 1, 2, 3 );
    }

    public static LdbcSnbBiQuery17FriendshipTriangles read17()
    {
        return new LdbcSnbBiQuery17FriendshipTriangles( "1" );
    }

    public static LdbcSnbBiQuery18PersonPostCounts read18()
    {
        return new LdbcSnbBiQuery18PersonPostCounts( 1, 1, Lists.newArrayList( "en", "fr" ), 1 );
    }

    public static LdbcSnbBiQuery21Zombies read21()
    {
        return new LdbcSnbBiQuery21Zombies( "1", 2, 3 );
    }

    public static LdbcSnbBiQuery22InternationalDialog read22()
    {
        return new LdbcSnbBiQuery22InternationalDialog( "\u4e35", "\u0634", 1 );
    }

    public static LdbcSnbBiQuery25WeightedPaths read25()
    {
        return new LdbcSnbBiQuery25WeightedPaths( 1, 2, 1, 2 );
    }

}
