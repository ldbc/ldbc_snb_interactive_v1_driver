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
        int year1 = 1;
        int month1 = 1;
        int limit1 = 1;

        int year2 = 2;
        int month2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery2TagEvolution query1a = new LdbcSnbBiQuery2TagEvolution(
                year1,
                month1,
                limit1 );
        LdbcSnbBiQuery2TagEvolution query1b = new LdbcSnbBiQuery2TagEvolution(
                year1,
                month1,
                limit1 );
        LdbcSnbBiQuery2TagEvolution query2a = new LdbcSnbBiQuery2TagEvolution(
                year2,
                month2,
                limit2 );
        LdbcSnbBiQuery2TagEvolution query3a = new LdbcSnbBiQuery2TagEvolution(
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
    public void ldbcQuery3ShouldDoEqualsCorrectly()
    {
        // Given
        String tagClass1 = "1";
        String country1 = "\u3055";
        int limit1 = 1;

        String tagClass2 = "2";
        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery3PopularCountryTopics
                query1a = new LdbcSnbBiQuery3PopularCountryTopics( tagClass1, country1, limit1 );
        LdbcSnbBiQuery3PopularCountryTopics
                query1b = new LdbcSnbBiQuery3PopularCountryTopics( tagClass1, country1, limit1 );
        LdbcSnbBiQuery3PopularCountryTopics
                query2a = new LdbcSnbBiQuery3PopularCountryTopics( tagClass2, country2, limit2 );
        LdbcSnbBiQuery3PopularCountryTopics
                query3a = new LdbcSnbBiQuery3PopularCountryTopics( tagClass2, country2, limit1 );

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
        String country1 = "\u3055";
        int limit1 = 1;

        String country2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery4TopCountryPosters query1a = new LdbcSnbBiQuery4TopCountryPosters(
                country1,
                limit1
        );
        LdbcSnbBiQuery4TopCountryPosters query1b = new LdbcSnbBiQuery4TopCountryPosters(
                country1,
                limit1
        );
        LdbcSnbBiQuery4TopCountryPosters query2a = new LdbcSnbBiQuery4TopCountryPosters(
                country2,
                limit2
        );
        LdbcSnbBiQuery4TopCountryPosters query3a = new LdbcSnbBiQuery4TopCountryPosters(
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
    public void ldbcQuery5ShouldDoEqualsCorrectly()
    {
        // Given
        String tag1 = "\u3055";
        int limit1 = 1;

        String tag2 = "\u4e35";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery5ActivePosters query1a = new LdbcSnbBiQuery5ActivePosters( tag1, limit1 );
        LdbcSnbBiQuery5ActivePosters query1b = new LdbcSnbBiQuery5ActivePosters( tag1, limit1 );
        LdbcSnbBiQuery5ActivePosters query2a = new LdbcSnbBiQuery5ActivePosters( tag2, limit2 );
        LdbcSnbBiQuery5ActivePosters query3a = new LdbcSnbBiQuery5ActivePosters( tag2, limit1 );

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
        LdbcSnbBiQuery6AuthoritativeUsers query1a = new LdbcSnbBiQuery6AuthoritativeUsers( tag1, limit1 );
        LdbcSnbBiQuery6AuthoritativeUsers query1b = new LdbcSnbBiQuery6AuthoritativeUsers( tag1, limit1 );
        LdbcSnbBiQuery6AuthoritativeUsers query2a = new LdbcSnbBiQuery6AuthoritativeUsers( tag2, limit2 );
        LdbcSnbBiQuery6AuthoritativeUsers query3a = new LdbcSnbBiQuery6AuthoritativeUsers( tag2, limit1 );

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
        LdbcSnbBiQuery7RelatedTopics query1a = new LdbcSnbBiQuery7RelatedTopics( tag1, limit1 );
        LdbcSnbBiQuery7RelatedTopics query1b = new LdbcSnbBiQuery7RelatedTopics( tag1, limit1 );
        LdbcSnbBiQuery7RelatedTopics query2a = new LdbcSnbBiQuery7RelatedTopics( tag2, limit2 );
        LdbcSnbBiQuery7RelatedTopics query3a = new LdbcSnbBiQuery7RelatedTopics( tag2, limit1 );

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
        long date1 = 1;
        int limit1 = 1;

        String tag2 = "\u4e35";
        long date2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery8TagPerson query1a = new LdbcSnbBiQuery8TagPerson( tag1, date1, limit1 );
        LdbcSnbBiQuery8TagPerson query1b = new LdbcSnbBiQuery8TagPerson( tag1, date1, limit1 );
        LdbcSnbBiQuery8TagPerson query2a = new LdbcSnbBiQuery8TagPerson( tag2, date2, limit2 );
        LdbcSnbBiQuery8TagPerson query3a = new LdbcSnbBiQuery8TagPerson( tag2, date1, limit1 );

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
        long startDate1 = 1;
        long endDate1 = 1;
        int limit1 = 1;

        long startDate2 = 2;
        long endDate2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery9TopThreadInitiators query1a = new LdbcSnbBiQuery9TopThreadInitiators(
                startDate1,
                endDate1,
                limit1 );
        LdbcSnbBiQuery9TopThreadInitiators query1b = new LdbcSnbBiQuery9TopThreadInitiators(
                startDate1,
                endDate1,
                limit1 );
        LdbcSnbBiQuery9TopThreadInitiators query2a = new LdbcSnbBiQuery9TopThreadInitiators(
                startDate2,
                endDate2,
                limit2 );
        LdbcSnbBiQuery9TopThreadInitiators query3a = new LdbcSnbBiQuery9TopThreadInitiators(
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
    public void ldbcQuery10ShouldDoEqualsCorrectly()
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
        LdbcSnbBiQuery10ExpertsInSocialCircle
                query1a = new LdbcSnbBiQuery10ExpertsInSocialCircle( personId1, country1, tagClass1, minPathDistance1,
                maxPathDistance1, limit1 );
        LdbcSnbBiQuery10ExpertsInSocialCircle
                query1b = new LdbcSnbBiQuery10ExpertsInSocialCircle( personId1, country1, tagClass1, minPathDistance1,
                maxPathDistance1, limit1 );
        LdbcSnbBiQuery10ExpertsInSocialCircle
                query2a = new LdbcSnbBiQuery10ExpertsInSocialCircle( personId2, country2, tagClass2, minPathDistance2,
                maxPathDistance2, limit2 );
        LdbcSnbBiQuery10ExpertsInSocialCircle
                query3a = new LdbcSnbBiQuery10ExpertsInSocialCircle( personId1, country1, tagClass2, minPathDistance1,
                maxPathDistance2, limit1 );

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
        String country1 = "\u3055";

        String country2 = "\u4e35";

        // When
        LdbcSnbBiQuery11FriendshipTriangles query1a = new LdbcSnbBiQuery11FriendshipTriangles( country1 );
        LdbcSnbBiQuery11FriendshipTriangles query1b = new LdbcSnbBiQuery11FriendshipTriangles( country1 );
        LdbcSnbBiQuery11FriendshipTriangles query2a = new LdbcSnbBiQuery11FriendshipTriangles( country2 );

        // Then
        assertThat( query1a, equalTo( query1b ) );
        assertThat( query1a, not( equalTo( query2a ) ) );
    }

    @Test
    public void ldbcQuery12ShouldDoEqualsCorrectly()
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
        LdbcSnbBiQuery12PersonPostCounts query1a = new LdbcSnbBiQuery12PersonPostCounts( date1, lengthThreshold1,
                languages1, limit1 );
        LdbcSnbBiQuery12PersonPostCounts query1b = new LdbcSnbBiQuery12PersonPostCounts( date1, lengthThreshold1,
                languages1, limit1 );
        LdbcSnbBiQuery12PersonPostCounts query2a = new LdbcSnbBiQuery12PersonPostCounts( date2, lengthThreshold2,
                languages2, limit2 );
        LdbcSnbBiQuery12PersonPostCounts query3a = new LdbcSnbBiQuery12PersonPostCounts( date1, lengthThreshold1,
                languages2, limit2 );

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
        long endDate1 = 1;
        int limit1 = 1;

        String country2 = "\u4e35";
        long endDate2 = 2;
        int limit2 = 2;

        // When
        LdbcSnbBiQuery13Zombies query1a = new LdbcSnbBiQuery13Zombies( country1, endDate1, limit1 );
        LdbcSnbBiQuery13Zombies query1b = new LdbcSnbBiQuery13Zombies( country1, endDate1, limit1 );
        LdbcSnbBiQuery13Zombies query2a = new LdbcSnbBiQuery13Zombies( country2, endDate2, limit2 );
        LdbcSnbBiQuery13Zombies query3a = new LdbcSnbBiQuery13Zombies( country2, endDate2, limit1 );

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
        String country11 = "\u3055";
        String country21 = "\u16a0";
        int limit1 = 1;

        String country12 = "\u4e35";
        String country22 = "tag";
        int limit2 = 2;

        // When
        LdbcSnbBiQuery14InternationalDialog
                query1a = new LdbcSnbBiQuery14InternationalDialog( country11, country21, limit1 );
        LdbcSnbBiQuery14InternationalDialog
                query1b = new LdbcSnbBiQuery14InternationalDialog( country11, country21, limit1 );
        LdbcSnbBiQuery14InternationalDialog
                query2a = new LdbcSnbBiQuery14InternationalDialog( country12, country22, limit2 );
        LdbcSnbBiQuery14InternationalDialog
                query3a = new LdbcSnbBiQuery14InternationalDialog( country12, country21, limit1 );

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
        long person1Id1 = 1;
        long person2Id1 = 2;
        long startDate1 = 1;
        long endDate1 = 2;

        long person1Id2 = 3;
        long person2Id2 = 4;
        long startDate2 = 3;
        long endDate2 = 4;

        // When
        LdbcSnbBiQuery15WeightedPaths query1a =
                new LdbcSnbBiQuery15WeightedPaths( person1Id1, person2Id1, startDate1, endDate1 );
        LdbcSnbBiQuery15WeightedPaths query1b =
                new LdbcSnbBiQuery15WeightedPaths( person1Id1, person2Id1, startDate1, endDate1 );
        LdbcSnbBiQuery15WeightedPaths query2a =
                new LdbcSnbBiQuery15WeightedPaths( person1Id1, person2Id2, startDate1, endDate2 );
        LdbcSnbBiQuery15WeightedPaths query3a =
                new LdbcSnbBiQuery15WeightedPaths( person1Id2, person2Id1, startDate2, endDate1 );

        // Then
        assertThat( query1a, equalTo( query1b ) );

        assertThat( query1a, not( equalTo( query2a ) ) );

        assertThat( query1a, not( equalTo( query3a ) ) );

        assertThat( query2a, not( equalTo( query3a ) ) );
    }
}
