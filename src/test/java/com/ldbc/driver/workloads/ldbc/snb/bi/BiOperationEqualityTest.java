package com.ldbc.driver.workloads.ldbc.snb.bi;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class BiOperationEqualityTest
{
    @Test
    public void ldbcQuery1ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;

        long date2 = 2;

        // When
        LdbcSnbBiQuery1PostingSummary query1a = new LdbcSnbBiQuery1PostingSummary( date1 );
        LdbcSnbBiQuery1PostingSummary query1b = new LdbcSnbBiQuery1PostingSummary( date1 );
        LdbcSnbBiQuery1PostingSummary query2a = new LdbcSnbBiQuery1PostingSummary( date2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
    }

    @Test
    public void ldbcQuery2ShouldDoEqualsCorrectly()
    {
        // Given
        long dateA1 = 1;
        long dateB1 = 1;
        String countryA1 = "a";
        String countryB1 = "a";
        int minMessageCount1 = 1;
        long endOfSimulationTime1 = 1l;
        int limit1 = 1;

        long dateA2 = 2;
        long dateB2 = 2;
        String countryA2 = "b";
        String countryB2 = "b";
        int minMessageCount2 = 2;
        long endOfSimulationTime2 = 2l;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery2TopTags query1a = new LdbcSnbBiQuery2TopTags(
                dateA1,
                dateB1,
                countryA1,
                countryB1,
                minMessageCount1,
                endOfSimulationTime1,
                limit1 );
        LdbcSnbBiQuery2TopTags query1b = new LdbcSnbBiQuery2TopTags(
                dateA1,
                dateB1,
                countryB1,
                countryB1,
                minMessageCount1,
                endOfSimulationTime1,
                limit1 );
        LdbcSnbBiQuery2TopTags query2a = new LdbcSnbBiQuery2TopTags(
                dateA2,
                dateB2,
                countryA2,
                countryB2,
                minMessageCount2,
                endOfSimulationTime2,
                limit2 );
        LdbcSnbBiQuery2TopTags query3a = new LdbcSnbBiQuery2TopTags(
                dateA1,
                dateB2,
                countryA2,
                countryB2,
                minMessageCount2,
                endOfSimulationTime2,
                limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery3ShouldDoEqualsCorrectly()
    {
        // Given
        int year1 = 1;
        int month1 = 1;

        int year2 = 2;
        int month2 = 2;

        // When
        LdbcSnbBiQuery3TagEvolution query1a = new LdbcSnbBiQuery3TagEvolution( year1, month1 );
        LdbcSnbBiQuery3TagEvolution query1b = new LdbcSnbBiQuery3TagEvolution( year1, month1 );
        LdbcSnbBiQuery3TagEvolution query2a = new LdbcSnbBiQuery3TagEvolution( year2, month2 );
        LdbcSnbBiQuery3TagEvolution query3a = new LdbcSnbBiQuery3TagEvolution( year1, month2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery4ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "1";
        String country1 = "\u3055";
        int limit1 = 1;

        String tagClass2 = "2";
        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery4PopularCountryTopics
                query1a = new LdbcSnbBiQuery4PopularCountryTopics( tagClass1, country1, limit1 );
        LdbcSnbBiQuery4PopularCountryTopics
                query1b = new LdbcSnbBiQuery4PopularCountryTopics( tagClass1, country1, limit1 );
        LdbcSnbBiQuery4PopularCountryTopics
                query2a = new LdbcSnbBiQuery4PopularCountryTopics( tagClass2, country2, limit2 );
        LdbcSnbBiQuery4PopularCountryTopics
                query3a = new LdbcSnbBiQuery4PopularCountryTopics( tagClass2, country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery5ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        int popularForumLimit1 = 1;
        int limit1 = 1;

        String country2 = "\u4e35";
        int popularForumLimit2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery5TopCountryPosters query1a = new LdbcSnbBiQuery5TopCountryPosters(
                country1,
                popularForumLimit1,
                limit1
        );
        LdbcSnbBiQuery5TopCountryPosters query1b = new LdbcSnbBiQuery5TopCountryPosters(
                country1,
                popularForumLimit1,
                limit1
        );
        LdbcSnbBiQuery5TopCountryPosters query2a = new LdbcSnbBiQuery5TopCountryPosters(
                country2,
                popularForumLimit2,
                limit2
        );
        LdbcSnbBiQuery5TopCountryPosters query3a = new LdbcSnbBiQuery5TopCountryPosters(
                country2,
                popularForumLimit1,
                limit1
        );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery6ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "\u3055";
        int limit1 = 1;

        String tag2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery6ActivePosters query1a = new LdbcSnbBiQuery6ActivePosters( tag1, limit1 );
        LdbcSnbBiQuery6ActivePosters query1b = new LdbcSnbBiQuery6ActivePosters( tag1, limit1 );
        LdbcSnbBiQuery6ActivePosters query2a = new LdbcSnbBiQuery6ActivePosters( tag2, limit2 );
        LdbcSnbBiQuery6ActivePosters query3a = new LdbcSnbBiQuery6ActivePosters( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery7ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "\u3055";
        int limit1 = 1;

        String tag2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery7AuthoritativeUsers query1a = new LdbcSnbBiQuery7AuthoritativeUsers( tag1, limit1 );
        LdbcSnbBiQuery7AuthoritativeUsers query1b = new LdbcSnbBiQuery7AuthoritativeUsers( tag1, limit1 );
        LdbcSnbBiQuery7AuthoritativeUsers query2a = new LdbcSnbBiQuery7AuthoritativeUsers( tag2, limit2 );
        LdbcSnbBiQuery7AuthoritativeUsers query3a = new LdbcSnbBiQuery7AuthoritativeUsers( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery8ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "\u3055";
        int limit1 = 1;

        String tag2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery8RelatedTopics query1a = new LdbcSnbBiQuery8RelatedTopics( tag1, limit1 );
        LdbcSnbBiQuery8RelatedTopics query1b = new LdbcSnbBiQuery8RelatedTopics( tag1, limit1 );
        LdbcSnbBiQuery8RelatedTopics query2a = new LdbcSnbBiQuery8RelatedTopics( tag2, limit2 );
        LdbcSnbBiQuery8RelatedTopics query3a = new LdbcSnbBiQuery8RelatedTopics( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery9ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClassA1 = "\u3055";
        String tagClassB1 = "1";
        int limit1 = 1;

        String tagClassA2 = "\u4e35";
        String tagClassB2 = "2";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery9RelatedForums query1a = new LdbcSnbBiQuery9RelatedForums( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery9RelatedForums query1b = new LdbcSnbBiQuery9RelatedForums( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery9RelatedForums query2a = new LdbcSnbBiQuery9RelatedForums( tagClassA2, tagClassB2, limit2 );
        LdbcSnbBiQuery9RelatedForums query3a = new LdbcSnbBiQuery9RelatedForums( tagClassA2, tagClassB2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery10ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "\u3055";
        int limit1 = 1;

        String tag2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery10TagPerson query1a = new LdbcSnbBiQuery10TagPerson( tag1, limit1 );
        LdbcSnbBiQuery10TagPerson query1b = new LdbcSnbBiQuery10TagPerson( tag1, limit1 );
        LdbcSnbBiQuery10TagPerson query2a = new LdbcSnbBiQuery10TagPerson( tag2, limit2 );
        LdbcSnbBiQuery10TagPerson query3a = new LdbcSnbBiQuery10TagPerson( tag2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery11ShouldDoEqualsCorrectly()
    {
        // Given
        String keyWord1 = "\u3055";
        String country1 = "\u16a0";
        int limit1 = 1;

        String keyWord2 = "\u4e35";
        String country2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery11UnrelatedReplies query1a = new LdbcSnbBiQuery11UnrelatedReplies( keyWord1, country1, limit1 );
        LdbcSnbBiQuery11UnrelatedReplies query1b = new LdbcSnbBiQuery11UnrelatedReplies( keyWord1, country1, limit1 );
        LdbcSnbBiQuery11UnrelatedReplies query2a = new LdbcSnbBiQuery11UnrelatedReplies( keyWord2, country2, limit2 );
        LdbcSnbBiQuery11UnrelatedReplies query3a = new LdbcSnbBiQuery11UnrelatedReplies( keyWord2, country1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery12ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery12TrendingPosts query1a = new LdbcSnbBiQuery12TrendingPosts( date1, limit1 );
        LdbcSnbBiQuery12TrendingPosts query1b = new LdbcSnbBiQuery12TrendingPosts( date1, limit1 );
        LdbcSnbBiQuery12TrendingPosts query2a = new LdbcSnbBiQuery12TrendingPosts( date2, limit2 );
        LdbcSnbBiQuery12TrendingPosts query3a = new LdbcSnbBiQuery12TrendingPosts( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery13ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery13PopularMonthlyTags query1a = new LdbcSnbBiQuery13PopularMonthlyTags( country1, limit1 );
        LdbcSnbBiQuery13PopularMonthlyTags query1b = new LdbcSnbBiQuery13PopularMonthlyTags( country1, limit1 );
        LdbcSnbBiQuery13PopularMonthlyTags query2a = new LdbcSnbBiQuery13PopularMonthlyTags( country2, limit2 );
        LdbcSnbBiQuery13PopularMonthlyTags query3a = new LdbcSnbBiQuery13PopularMonthlyTags( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery14ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery14TopThreadInitiators query1a = new LdbcSnbBiQuery14TopThreadInitiators( date1, limit1 );
        LdbcSnbBiQuery14TopThreadInitiators query1b = new LdbcSnbBiQuery14TopThreadInitiators( date1, limit1 );
        LdbcSnbBiQuery14TopThreadInitiators query2a = new LdbcSnbBiQuery14TopThreadInitiators( date2, limit2 );
        LdbcSnbBiQuery14TopThreadInitiators query3a = new LdbcSnbBiQuery14TopThreadInitiators( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery15ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery15SocialNormals query1a = new LdbcSnbBiQuery15SocialNormals( country1, limit1 );
        LdbcSnbBiQuery15SocialNormals query1b = new LdbcSnbBiQuery15SocialNormals( country1, limit1 );
        LdbcSnbBiQuery15SocialNormals query2a = new LdbcSnbBiQuery15SocialNormals( country2, limit2 );
        LdbcSnbBiQuery15SocialNormals query3a = new LdbcSnbBiQuery15SocialNormals( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery16ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "\u3055";
        String country1 = "\u16a0";
        int limit1 = 1;

        String tagClass2 = "\u4e35";
        String country2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query1a = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClass1, country1, limit1 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query1b = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClass1, country1, limit1 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query2a = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClass2, country2, limit2 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query3a = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClass2, country1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery17ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery17FriendshipTriangles query1a = new LdbcSnbBiQuery17FriendshipTriangles( country1, limit1 );
        LdbcSnbBiQuery17FriendshipTriangles query1b = new LdbcSnbBiQuery17FriendshipTriangles( country1, limit1 );
        LdbcSnbBiQuery17FriendshipTriangles query2a = new LdbcSnbBiQuery17FriendshipTriangles( country2, limit2 );
        LdbcSnbBiQuery17FriendshipTriangles query3a = new LdbcSnbBiQuery17FriendshipTriangles( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery18ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery18PersonPostCounts query1a = new LdbcSnbBiQuery18PersonPostCounts( date1, limit1 );
        LdbcSnbBiQuery18PersonPostCounts query1b = new LdbcSnbBiQuery18PersonPostCounts( date1, limit1 );
        LdbcSnbBiQuery18PersonPostCounts query2a = new LdbcSnbBiQuery18PersonPostCounts( date2, limit2 );
        LdbcSnbBiQuery18PersonPostCounts query3a = new LdbcSnbBiQuery18PersonPostCounts( date1, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery19ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClassA1 = "\u3055";
        String tagClassB1 = "\u16a0";
        int limit1 = 1;

        String tagClassA2 = "\u4e35";
        String tagClassB2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query1a = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query1b = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClassA1, tagClassB1, limit1 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query2a = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClassA2, tagClassB2, limit2 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query3a = new LdbcSnbBiQuery16ExpertsInSocialCircle( tagClassA2, tagClassB1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery20ShouldDoEqualsCorrectly()
    {
        // Given
        int limit1 = 1;

        int limit2 = 2;

        // When
        LdbcSnbBiQuery20HighLevelTopics query1a = new LdbcSnbBiQuery20HighLevelTopics( limit1 );
        LdbcSnbBiQuery20HighLevelTopics query1b = new LdbcSnbBiQuery20HighLevelTopics( limit1 );
        LdbcSnbBiQuery20HighLevelTopics query2a = new LdbcSnbBiQuery20HighLevelTopics( limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
    }

    @Test
    public void ldbcQuery21ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery21Zombies query1a = new LdbcSnbBiQuery21Zombies( country1, limit1 );
        LdbcSnbBiQuery21Zombies query1b = new LdbcSnbBiQuery21Zombies( country1, limit1 );
        LdbcSnbBiQuery21Zombies query2a = new LdbcSnbBiQuery21Zombies( country2, limit2 );
        LdbcSnbBiQuery21Zombies query3a = new LdbcSnbBiQuery21Zombies( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery22ShouldDoEqualsCorrectly()
    {
        // Given
        String countryA1 = "\u3055";
        String countryB1 = "\u16a0";
        int limit1 = 1;

        String countryA2 = "\u4e35";
        String countryB2 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery22InternationalDialog
                query1a = new LdbcSnbBiQuery22InternationalDialog( countryA1, countryB1, limit1 );
        LdbcSnbBiQuery22InternationalDialog
                query1b = new LdbcSnbBiQuery22InternationalDialog( countryA1, countryB1, limit1 );
        LdbcSnbBiQuery22InternationalDialog
                query2a = new LdbcSnbBiQuery22InternationalDialog( countryA2, countryB2, limit2 );
        LdbcSnbBiQuery22InternationalDialog
                query3a = new LdbcSnbBiQuery22InternationalDialog( countryA2, countryB1, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery23ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery23HolidayDestinations query1a = new LdbcSnbBiQuery23HolidayDestinations( country1, limit1 );
        LdbcSnbBiQuery23HolidayDestinations query1b = new LdbcSnbBiQuery23HolidayDestinations( country1, limit1 );
        LdbcSnbBiQuery23HolidayDestinations query2a = new LdbcSnbBiQuery23HolidayDestinations( country2, limit2 );
        LdbcSnbBiQuery23HolidayDestinations query3a = new LdbcSnbBiQuery23HolidayDestinations( country2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }

    @Test
    public void ldbcQuery24ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "\u3055";
        int limit1 = 1;

        String tagClass2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery24MessagesByTopic query1a = new LdbcSnbBiQuery24MessagesByTopic( tagClass1, limit1 );
        LdbcSnbBiQuery24MessagesByTopic query1b = new LdbcSnbBiQuery24MessagesByTopic( tagClass1, limit1 );
        LdbcSnbBiQuery24MessagesByTopic query2a = new LdbcSnbBiQuery24MessagesByTopic( tagClass2, limit2 );
        LdbcSnbBiQuery24MessagesByTopic query3a = new LdbcSnbBiQuery24MessagesByTopic( tagClass2, limit1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
        assertThat( query1a, not( equalTo( query3a ) ) );
        assertThat( query2a, not( equalTo( query3a ) ) );
    }
}