package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

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
        long startDate1 = 1;
        long endDate1 = 1;
        String country11 = "a";
        String country21 = "b";
        int limit1 = 1;

        long startDate2 = 2;
        long endDate2 = 2;
        String country12 = "a";
        String country22 = "c";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery2TopTags query1a = new LdbcSnbBiQuery2TopTags(
                startDate1,
                endDate1,
                country11,
                country21,
                limit1 );
        LdbcSnbBiQuery2TopTags query1b = new LdbcSnbBiQuery2TopTags(
                startDate1,
                endDate1,
                country11,
                country21,
                limit1 );
        LdbcSnbBiQuery2TopTags query2a = new LdbcSnbBiQuery2TopTags(
                startDate2,
                endDate2,
                country12,
                country22,
                limit2 );
        LdbcSnbBiQuery2TopTags query3a = new LdbcSnbBiQuery2TopTags(
                startDate1,
                endDate2,
                country11,
                country22,
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
        int limit1 = 1;

        int year2 = 2;
        int month2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery3TagEvolution query1a = new LdbcSnbBiQuery3TagEvolution(
                year1,
                month1,
                limit1 );
        LdbcSnbBiQuery3TagEvolution query1b = new LdbcSnbBiQuery3TagEvolution(
                year1,
                month1,
                limit1 );
        LdbcSnbBiQuery3TagEvolution query2a = new LdbcSnbBiQuery3TagEvolution(
                year2,
                month2,
                limit2 );
        LdbcSnbBiQuery3TagEvolution query3a = new LdbcSnbBiQuery3TagEvolution(
                year1,
                month2,
                limit1 );

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
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery5TopCountryPosters query1a = new LdbcSnbBiQuery5TopCountryPosters(
                country1,
                limit1
        );
        LdbcSnbBiQuery5TopCountryPosters query1b = new LdbcSnbBiQuery5TopCountryPosters(
                country1,
                limit1
        );
        LdbcSnbBiQuery5TopCountryPosters query2a = new LdbcSnbBiQuery5TopCountryPosters(
                country2,
                limit2
        );
        LdbcSnbBiQuery5TopCountryPosters query3a = new LdbcSnbBiQuery5TopCountryPosters(
                country2,
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
        String tagClass11 = "\u3055";
        String tagClass21 = "1";
        int threshold1 = 1;
        int limit1 = 1;

        String tagClass12 = "\u4e35";
        String tagClass22 = "2";
        int threshold2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery9RelatedForums query1a = new LdbcSnbBiQuery9RelatedForums(
                tagClass11,
                tagClass21,
                threshold1,
                limit1 );
        LdbcSnbBiQuery9RelatedForums query1b = new LdbcSnbBiQuery9RelatedForums(
                tagClass11,
                tagClass21,
                threshold1,
                limit1 );
        LdbcSnbBiQuery9RelatedForums query2a = new LdbcSnbBiQuery9RelatedForums(
                tagClass12,
                tagClass22,
                threshold2,
                limit2 );
        LdbcSnbBiQuery9RelatedForums query3a = new LdbcSnbBiQuery9RelatedForums(
                tagClass12,
                tagClass22,
                threshold2,
                limit1 );

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
        long date1 = 1;
        int limit1 = 1;

        String tag2 = "\u4e35";
        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery10TagPerson query1a = new LdbcSnbBiQuery10TagPerson( tag1, date1, limit1 );
        LdbcSnbBiQuery10TagPerson query1b = new LdbcSnbBiQuery10TagPerson( tag1, date1, limit1 );
        LdbcSnbBiQuery10TagPerson query2a = new LdbcSnbBiQuery10TagPerson( tag2, date2, limit2 );
        LdbcSnbBiQuery10TagPerson query3a = new LdbcSnbBiQuery10TagPerson( tag2, date1, limit1 );

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
        String country1 = "\u16a0";
        List<String> blacklist1 = Lists.newArrayList( "\u3055" );
        int limit1 = 1;

        String country2 = "tag";
        List<String> blacklist2 = Lists.newArrayList( "\u4e35" );
        int limit2 = 2;

        // When
        LdbcSnbBiQuery11UnrelatedReplies query1a = new LdbcSnbBiQuery11UnrelatedReplies( country1, blacklist1, limit1 );
        LdbcSnbBiQuery11UnrelatedReplies query1b = new LdbcSnbBiQuery11UnrelatedReplies( country1, blacklist1, limit1 );
        LdbcSnbBiQuery11UnrelatedReplies query2a = new LdbcSnbBiQuery11UnrelatedReplies( country2, blacklist2, limit2 );
        LdbcSnbBiQuery11UnrelatedReplies query3a = new LdbcSnbBiQuery11UnrelatedReplies( country1, blacklist2, limit1 );

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
        int likeThreshold1 = 1;
        int limit1 = 1;

        long date2 = 2;
        int likeThreshold2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery12TrendingPosts query1a = new LdbcSnbBiQuery12TrendingPosts( date1, likeThreshold1, limit1 );
        LdbcSnbBiQuery12TrendingPosts query1b = new LdbcSnbBiQuery12TrendingPosts( date1, likeThreshold1, limit1 );
        LdbcSnbBiQuery12TrendingPosts query2a = new LdbcSnbBiQuery12TrendingPosts( date2, likeThreshold2, limit2 );
        LdbcSnbBiQuery12TrendingPosts query3a = new LdbcSnbBiQuery12TrendingPosts( date1, likeThreshold2, limit2 );

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
        long startDate1 = 1;
        long endDate1 = 1;
        int limit1 = 1;

        long startDate2 = 2;
        long endDate2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery14TopThreadInitiators query1a = new LdbcSnbBiQuery14TopThreadInitiators(
                startDate1,
                endDate1,
                limit1 );
        LdbcSnbBiQuery14TopThreadInitiators query1b = new LdbcSnbBiQuery14TopThreadInitiators(
                startDate1,
                endDate1,
                limit1 );
        LdbcSnbBiQuery14TopThreadInitiators query2a = new LdbcSnbBiQuery14TopThreadInitiators(
                startDate2,
                endDate2,
                limit2 );
        LdbcSnbBiQuery14TopThreadInitiators query3a = new LdbcSnbBiQuery14TopThreadInitiators(
                startDate1,
                endDate2,
                limit2 );

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
        long personId1 = 1;
        String country1 = "\u16a0";
        String tagClass1 = "\u3055";
        int minPathDistance1 = 1;
        int maxPathDistance1 = 1;
        int limit1 = 1;

        long personId2 = 2;
        String country2 = "tag";
        String tagClass2 = "\u4e35";
        int minPathDistance2 = 2;
        int maxPathDistance2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query1a = new LdbcSnbBiQuery16ExpertsInSocialCircle( personId1, country1, tagClass1, minPathDistance1,
                maxPathDistance1, limit1 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query1b = new LdbcSnbBiQuery16ExpertsInSocialCircle( personId1, country1, tagClass1, minPathDistance1,
                maxPathDistance1, limit1 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query2a = new LdbcSnbBiQuery16ExpertsInSocialCircle( personId2, country2, tagClass2, minPathDistance2,
                maxPathDistance2, limit2 );
        LdbcSnbBiQuery16ExpertsInSocialCircle
                query3a = new LdbcSnbBiQuery16ExpertsInSocialCircle( personId1, country1, tagClass2, minPathDistance1,
                maxPathDistance2, limit1 );

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

        String country2 = "\u4e35";

        // When
        LdbcSnbBiQuery17FriendshipTriangles query1a = new LdbcSnbBiQuery17FriendshipTriangles( country1 );
        LdbcSnbBiQuery17FriendshipTriangles query1b = new LdbcSnbBiQuery17FriendshipTriangles( country1 );
        LdbcSnbBiQuery17FriendshipTriangles query2a = new LdbcSnbBiQuery17FriendshipTriangles( country2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
    }

    @Test
    public void ldbcQuery18ShouldDoEqualsCorrectly()
    {
        // Given
        long date1 = 1;
        int lengthThreshold1 = 1;
        List<String> languages1 = Lists.newArrayList( "en" );
        int limit1 = 1;

        long date2 = 2;
        int lengthThreshold2 = 1;
        List<String> languages2 = Lists.newArrayList( "en", "fr" );
        int limit2 = 2;

        // When
        LdbcSnbBiQuery18PersonPostCounts query1a = new LdbcSnbBiQuery18PersonPostCounts( date1, lengthThreshold1,
                languages1, limit1 );
        LdbcSnbBiQuery18PersonPostCounts query1b = new LdbcSnbBiQuery18PersonPostCounts( date1, lengthThreshold1,
                languages1, limit1 );
        LdbcSnbBiQuery18PersonPostCounts query2a = new LdbcSnbBiQuery18PersonPostCounts( date2, lengthThreshold2,
                languages2, limit2 );
        LdbcSnbBiQuery18PersonPostCounts query3a = new LdbcSnbBiQuery18PersonPostCounts( date1, lengthThreshold1,
                languages2, limit2 );

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
        long date1 = 1;
        String tagClass11 = "\u3055";
        String tagClass21 = "\u16a0";
        int limit1 = 1;

        long date2 = 2;
        String tagClass12 = "\u4e35";
        String tagClass22 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery19StrangerInteraction
                query1a = new LdbcSnbBiQuery19StrangerInteraction( date1, tagClass11, tagClass21, limit1 );
        LdbcSnbBiQuery19StrangerInteraction
                query1b = new LdbcSnbBiQuery19StrangerInteraction( date1, tagClass11, tagClass21, limit1 );
        LdbcSnbBiQuery19StrangerInteraction
                query2a = new LdbcSnbBiQuery19StrangerInteraction( date2, tagClass12, tagClass22, limit2 );
        LdbcSnbBiQuery19StrangerInteraction
                query3a = new LdbcSnbBiQuery19StrangerInteraction( date1, tagClass12, tagClass21, limit1 );

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
        List<String> tagClasses1 = Lists.newArrayList( "1", "2" );
        int limit1 = 1;

        List<String> tagClasses2 = Lists.newArrayList( "1" );
        int limit2 = 1;

        // When
        LdbcSnbBiQuery20HighLevelTopics query1a = new LdbcSnbBiQuery20HighLevelTopics( tagClasses1, limit1 );
        LdbcSnbBiQuery20HighLevelTopics query1b = new LdbcSnbBiQuery20HighLevelTopics( tagClasses1, limit1 );
        LdbcSnbBiQuery20HighLevelTopics query2a = new LdbcSnbBiQuery20HighLevelTopics( tagClasses2, limit2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
    }

    @Test
    public void ldbcQuery21ShouldDoEqualsCorrectly()
    {
        // Given
        String country1 = "\u3055";
        long endDate1 = 1;
        int limit1 = 1;

        String country2 = "\u4e35";
        long endDate2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery21Zombies query1a = new LdbcSnbBiQuery21Zombies( country1, endDate1, limit1 );
        LdbcSnbBiQuery21Zombies query1b = new LdbcSnbBiQuery21Zombies( country1, endDate1, limit1 );
        LdbcSnbBiQuery21Zombies query2a = new LdbcSnbBiQuery21Zombies( country2, endDate2, limit2 );
        LdbcSnbBiQuery21Zombies query3a = new LdbcSnbBiQuery21Zombies( country2, endDate2, limit1 );

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
        String country11 = "\u3055";
        String country21 = "\u16a0";
        int limit1 = 1;

        String country12 = "\u4e35";
        String country22 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery22InternationalDialog
                query1a = new LdbcSnbBiQuery22InternationalDialog( country11, country21, limit1 );
        LdbcSnbBiQuery22InternationalDialog
                query1b = new LdbcSnbBiQuery22InternationalDialog( country11, country21, limit1 );
        LdbcSnbBiQuery22InternationalDialog
                query2a = new LdbcSnbBiQuery22InternationalDialog( country12, country22, limit2 );
        LdbcSnbBiQuery22InternationalDialog
                query3a = new LdbcSnbBiQuery22InternationalDialog( country12, country21, limit1 );

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

    @Test
    public void ldbcQuery25ShouldDoEqualsCorrectly()
    {
        // Given
        long person1Id1 = 1;
        long person2Id1 = 2;
        long startDate1 = 1;
        long endDate1 = 2;

        long person1Id2 = 3;
        long person2Id2 = 4;
        long startDate2 = 3;
        long endDate2 = 4;

        // When
        LdbcSnbBiQuery25WeightedPaths query1a =
                new LdbcSnbBiQuery25WeightedPaths( person1Id1, person2Id1, startDate1, endDate1 );
        LdbcSnbBiQuery25WeightedPaths query1b =
                new LdbcSnbBiQuery25WeightedPaths( person1Id1, person2Id1, startDate1, endDate1 );
        LdbcSnbBiQuery25WeightedPaths query2a =
                new LdbcSnbBiQuery25WeightedPaths( person1Id1, person2Id2, startDate1, endDate2 );
        LdbcSnbBiQuery25WeightedPaths query3a =
                new LdbcSnbBiQuery25WeightedPaths( person1Id2, person2Id1, startDate2, endDate1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );

        assertThat( query1a, not( equalTo( query2a ) ) );

        assertThat( query1a, not( equalTo( query3a ) ) );

        assertThat( query2a, not( equalTo( query3a ) ) );
    }
}
