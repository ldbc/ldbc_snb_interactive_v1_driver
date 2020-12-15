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

    public static LdbcSnbBiQuery2TagEvolution read2()
    {
        return new LdbcSnbBiQuery2TagEvolution( 1, 2, "tagClass", 3 );
    }

    public static LdbcSnbBiQuery3PopularCountryTopics read3()
    {
        return new LdbcSnbBiQuery3PopularCountryTopics( "tagClass", "1", 2 );
    }

    public static LdbcSnbBiQuery4TopCountryPosters read4()
    {
        return new LdbcSnbBiQuery4TopCountryPosters( "\u16a0", 1 );
    }

    public static LdbcSnbBiQuery5ActivePosters read5()
    {
        return new LdbcSnbBiQuery5ActivePosters( "\u3055", 1 );
    }

    public static LdbcSnbBiQuery6AuthoritativeUsers read6()
    {
        return new LdbcSnbBiQuery6AuthoritativeUsers( "\u4e35", 1 );
    }

    public static LdbcSnbBiQuery7RelatedTopics read7()
    {
        return new LdbcSnbBiQuery7RelatedTopics( "\u0634", 1 );
    }

    public static LdbcSnbBiQuery8TagPerson read8()
    {
        return new LdbcSnbBiQuery8TagPerson( "\u3055", 1, 1 );
    }

    public static LdbcSnbBiQuery9TopThreadInitiators read9()
    {
        return new LdbcSnbBiQuery9TopThreadInitiators( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery10ExpertsInSocialCircle read10()
    {
        return new LdbcSnbBiQuery10ExpertsInSocialCircle( 0, "1", "2", 1, 2, 3 );
    }

    public static LdbcSnbBiQuery11FriendshipTriangles read11()
    {
        return new LdbcSnbBiQuery11FriendshipTriangles( "1" );
    }

    public static LdbcSnbBiQuery12PersonPostCounts read12()
    {
        return new LdbcSnbBiQuery12PersonPostCounts( 1, 1, Lists.newArrayList( "en", "fr" ), 1 );
    }

    public static LdbcSnbBiQuery13Zombies read13()
    {
        return new LdbcSnbBiQuery13Zombies( "1", 2, 3 );
    }

    public static LdbcSnbBiQuery14InternationalDialog read14()
    {
        return new LdbcSnbBiQuery14InternationalDialog( "\u4e35", "\u0634", 1 );
    }

    public static LdbcSnbBiQuery15WeightedPaths read15()
    {
        return new LdbcSnbBiQuery15WeightedPaths( 1, 2, 1, 2 );
    }

    public static LdbcSnbBiQuery16FakeNewsDetection read16()
    {
        return new LdbcSnbBiQuery16FakeNewsDetection( "My_Tag1", 2L, "My_Tag2", 3L, 5, 20 );
    }

    public static LdbcSnbBiQuery17InformationPropagationAnalysis read17()
    {
        return new LdbcSnbBiQuery17InformationPropagationAnalysis( "My_Tag", 5, 20 );
    }

    public static LdbcSnbBiQuery18FriendRecommendation read18()
    {
        return new LdbcSnbBiQuery18FriendRecommendation( 1L, "My_Tag", 20 );
    }

    public static LdbcSnbBiQuery19InteractionPathBetweenCities read19()
    {
        return new LdbcSnbBiQuery19InteractionPathBetweenCities( 1L, 2L, 20 );
    }

    public static LdbcSnbBiQuery20Recruitment read20()
    {
        return new LdbcSnbBiQuery20Recruitment( "My_Company", 2 , 20 );
    }

}
