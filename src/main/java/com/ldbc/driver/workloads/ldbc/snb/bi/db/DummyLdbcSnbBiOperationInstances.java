package com.ldbc.driver.workloads.ldbc.snb.bi.db;

import com.google.common.collect.Lists;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery10TagPerson;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery11UnrelatedReplies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery12TrendingPosts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery13PopularMonthlyTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery14TopThreadInitiators;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery15SocialNormals;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery16ExpertsInSocialCircle;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery17FriendshipTriangles;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery18PersonPostCounts;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery19StrangerInteraction;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery1PostingSummary;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery20HighLevelTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery21Zombies;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery22InternationalDialog;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery23HolidayDestinations;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery24MessagesByTopic;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery2TopTags;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery3TagEvolution;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery4PopularCountryTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery5TopCountryPosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery6ActivePosters;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery7AuthoritativeUsers;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery8RelatedTopics;
import com.ldbc.driver.workloads.ldbc.snb.bi.LdbcSnbBiQuery9RelatedForums;

public class DummyLdbcSnbBiOperationInstances
{

    /*
    LONG READS
     */

    public static LdbcSnbBiQuery1PostingSummary read1()
    {
        return new LdbcSnbBiQuery1PostingSummary( 1 );
    }

    public static LdbcSnbBiQuery2TopTags read2()
    {
        return new LdbcSnbBiQuery2TopTags( 1, 2, Lists.newArrayList( "3", "4" ), 5, 6, 7 );
    }

    public static LdbcSnbBiQuery3TagEvolution read3()
    {
        return new LdbcSnbBiQuery3TagEvolution( 1, 2, 3, 4, 5 );
    }

    public static LdbcSnbBiQuery4PopularCountryTopics read4()
    {
        return new LdbcSnbBiQuery4PopularCountryTopics( "tagClass", "1", 2 );
    }

    public static LdbcSnbBiQuery5TopCountryPosters read5()
    {
        return new LdbcSnbBiQuery5TopCountryPosters( "\u16a0", 1, 2 );
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

    public static LdbcSnbBiQuery9RelatedForums read9()
    {
        return new LdbcSnbBiQuery9RelatedForums( "1", "\u0634", 1, 2 );
    }

    public static LdbcSnbBiQuery10TagPerson read10()
    {
        return new LdbcSnbBiQuery10TagPerson( "\u3055", 1 );
    }

    public static LdbcSnbBiQuery11UnrelatedReplies read11()
    {
        return new LdbcSnbBiQuery11UnrelatedReplies( "1", Lists.newArrayList( "2" ), 3 );
    }

    public static LdbcSnbBiQuery12TrendingPosts read12()
    {
        return new LdbcSnbBiQuery12TrendingPosts( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery13PopularMonthlyTags read13()
    {
        return new LdbcSnbBiQuery13PopularMonthlyTags( "\u4e35", 1 );
    }

    public static LdbcSnbBiQuery14TopThreadInitiators read14()
    {
        return new LdbcSnbBiQuery14TopThreadInitiators( 1, 2, 3 );
    }

    public static LdbcSnbBiQuery15SocialNormals read15()
    {
        return new LdbcSnbBiQuery15SocialNormals( "1", 2 );
    }

    public static LdbcSnbBiQuery16ExpertsInSocialCircle read16()
    {
        return new LdbcSnbBiQuery16ExpertsInSocialCircle( 0, "1", "2", 3 );
    }

    public static LdbcSnbBiQuery17FriendshipTriangles read17()
    {
        return new LdbcSnbBiQuery17FriendshipTriangles( "1" );
    }

    public static LdbcSnbBiQuery18PersonPostCounts read18()
    {
        return new LdbcSnbBiQuery18PersonPostCounts( 1, 2 );
    }

    public static LdbcSnbBiQuery19StrangerInteraction read19()
    {
        return new LdbcSnbBiQuery19StrangerInteraction( 1, "\u4e35", "\u16a0", 1 );
    }

    public static LdbcSnbBiQuery20HighLevelTopics read20()
    {
        return new LdbcSnbBiQuery20HighLevelTopics( Lists.newArrayList( "a", "b" ), 1 );
    }

    public static LdbcSnbBiQuery21Zombies read21()
    {
        return new LdbcSnbBiQuery21Zombies( "1", 2, 3, 4 );
    }

    public static LdbcSnbBiQuery22InternationalDialog read22()
    {
        return new LdbcSnbBiQuery22InternationalDialog( "\u4e35", "\u0634", 1 );
    }

    public static LdbcSnbBiQuery23HolidayDestinations read23()
    {
        return new LdbcSnbBiQuery23HolidayDestinations( "\u4e35", 1 );
    }

    public static LdbcSnbBiQuery24MessagesByTopic read24()
    {
        return new LdbcSnbBiQuery24MessagesByTopic( "1", 2 );
    }
}
