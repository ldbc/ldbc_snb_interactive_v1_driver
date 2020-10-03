package com.ldbc.driver.workloads.ldbc.snb.bi;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class BiOperationResultEqualityTest
{
    @Test
    public void ldbcQuery1ResultShouldDoEqualsCorrectly()
    {
        int messageYear1 = 1;
        boolean isComment1 = false;
        int lengthCategory1 = 2;
        int messageCount1 = 3;
        int averageMessageLength1 = 4;
        int sumMessageLength1 = 5;
        float percentageOfMessages1 = 6;

        int messageYear2 = 7;
        boolean isComment2 = true;
        int lengthCategory2 = 8;
        int messageCount2 = 9;
        int averageMessageLength2 = 10;
        int sumMessageLength2 = 11;
        float percentageOfMessages2 = 12;


        LdbcSnbBiQuery1PostingSummaryResult result1a = new LdbcSnbBiQuery1PostingSummaryResult(
                messageYear1,
                isComment1,
                lengthCategory1,
                messageCount1,
                averageMessageLength1,
                sumMessageLength1,
                percentageOfMessages1
        );
        LdbcSnbBiQuery1PostingSummaryResult result1b = new LdbcSnbBiQuery1PostingSummaryResult(
                messageYear1,
                isComment1,
                lengthCategory1,
                messageCount1,
                averageMessageLength1,
                sumMessageLength1,
                percentageOfMessages1
        );
        LdbcSnbBiQuery1PostingSummaryResult result2a = new LdbcSnbBiQuery1PostingSummaryResult(
                messageYear2,
                isComment2,
                lengthCategory2,
                messageCount2,
                averageMessageLength2,
                sumMessageLength2,
                percentageOfMessages2
        );
        LdbcSnbBiQuery1PostingSummaryResult result3a = new LdbcSnbBiQuery1PostingSummaryResult(
                messageYear2,
                isComment2,
                lengthCategory2,
                messageCount2,
                averageMessageLength2,
                sumMessageLength2,
                percentageOfMessages1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery2ResultShouldDoEqualsCorrectly()
    {
        String tagName1 = "\u3055";
        int countMonth11 = 1;
        int countMonth21 = 2;
        int diff1 = 3;

        String tagName2 = "\u4e35";
        int countMonth12 = 4;
        int countMonth22 = 5;
        int diff2 = 6;

        LdbcSnbBiQuery2TagEvolutionResult result1a = new LdbcSnbBiQuery2TagEvolutionResult(
                tagName1,
                countMonth11,
                countMonth21,
                diff1
        );
        LdbcSnbBiQuery2TagEvolutionResult result1b = new LdbcSnbBiQuery2TagEvolutionResult(
                tagName1,
                countMonth11,
                countMonth21,
                diff1
        );
        LdbcSnbBiQuery2TagEvolutionResult result2a = new LdbcSnbBiQuery2TagEvolutionResult(
                tagName2,
                countMonth12,
                countMonth22,
                diff2
        );
        LdbcSnbBiQuery2TagEvolutionResult result3a = new LdbcSnbBiQuery2TagEvolutionResult(
                tagName2,
                countMonth12,
                countMonth22,
                diff1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery3ResultShouldDoEqualsCorrectly()
    {
        long forumId1 = Long.MAX_VALUE;
        String forumTitle1 = "\u3055";
        long forumCreationDate1 = Long.MIN_VALUE;
        long personId1 = 1;
        int postCount1 = 2;

        long forumId2 = 3;
        String forumTitle2 = "\u4e35";
        long forumCreationDate2 = 4;
        long personId2 = 5;
        int postCount2 = 6;

        LdbcSnbBiQuery3PopularCountryTopicsResult result1a = new LdbcSnbBiQuery3PopularCountryTopicsResult(
                forumId1,
                forumTitle1,
                forumCreationDate1,
                personId1,
                postCount1
        );
        LdbcSnbBiQuery3PopularCountryTopicsResult result1b = new LdbcSnbBiQuery3PopularCountryTopicsResult(
                forumId1,
                forumTitle1,
                forumCreationDate1,
                personId1,
                postCount1
        );
        LdbcSnbBiQuery3PopularCountryTopicsResult result2a = new LdbcSnbBiQuery3PopularCountryTopicsResult(
                forumId2,
                forumTitle2,
                forumCreationDate2,
                personId2,
                postCount2
        );
        LdbcSnbBiQuery3PopularCountryTopicsResult result3a = new LdbcSnbBiQuery3PopularCountryTopicsResult(
                forumId2,
                forumTitle2,
                forumCreationDate2,
                personId2,
                postCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery4ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        String personFirstName1 = "\u3055";
        String personLastName1 = "\u4e35";
        long personCreationDate1 = 2;
        int postCount1 = Integer.MAX_VALUE;

        long personId2 = 3;
        String personFirstName2 = "4";
        String personLastName2 = "5";
        long personCreationDate2 = 6;
        int count2 = Integer.MIN_VALUE;

        LdbcSnbBiQuery4TopCountryPostersResult result1a = new LdbcSnbBiQuery4TopCountryPostersResult(
                personId1,
                personFirstName1,
                personLastName1,
                personCreationDate1,
                postCount1
        );
        LdbcSnbBiQuery4TopCountryPostersResult result1b = new LdbcSnbBiQuery4TopCountryPostersResult(
                personId1,
                personFirstName1,
                personLastName1,
                personCreationDate1,
                postCount1
        );
        LdbcSnbBiQuery4TopCountryPostersResult result2a = new LdbcSnbBiQuery4TopCountryPostersResult(
                personId2,
                personFirstName2,
                personLastName2,
                personCreationDate2,
                count2
        );
        LdbcSnbBiQuery4TopCountryPostersResult result3a = new LdbcSnbBiQuery4TopCountryPostersResult(
                personId2,
                personFirstName2,
                personLastName2,
                personCreationDate2,
                postCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery5ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int replyCount1 = 2;
        int likeCount1 = 3;
        int messageCount1 = 4;
        int score1 = 5;

        long personId2 = 6;
        int replyCount2 = 7;
        int likeCount2 = 8;
        int messageCount2 = 9;
        int score2 = 10;

        LdbcSnbBiQuery5ActivePostersResult result1a = new LdbcSnbBiQuery5ActivePostersResult(
                personId1,
                replyCount1,
                likeCount1,
                messageCount1,
                score1
        );
        LdbcSnbBiQuery5ActivePostersResult result1b = new LdbcSnbBiQuery5ActivePostersResult(
                personId1,
                replyCount1,
                likeCount1,
                messageCount1,
                score1
        );
        LdbcSnbBiQuery5ActivePostersResult result2a = new LdbcSnbBiQuery5ActivePostersResult(
                personId2,
                replyCount2,
                likeCount2,
                messageCount2,
                score2
        );
        LdbcSnbBiQuery5ActivePostersResult result3a = new LdbcSnbBiQuery5ActivePostersResult(
                personId2,
                replyCount2,
                likeCount2,
                messageCount2,
                score1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery6ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int authorityScore1 = 2;

        long personId2 = 3;
        int authorityScore2 = 4;

        LdbcSnbBiQuery6AuthoritativeUsersResult result1a = new LdbcSnbBiQuery6AuthoritativeUsersResult(
                personId1,
                authorityScore1
        );
        LdbcSnbBiQuery6AuthoritativeUsersResult result1b = new LdbcSnbBiQuery6AuthoritativeUsersResult(
                personId1,
                authorityScore1
        );
        LdbcSnbBiQuery6AuthoritativeUsersResult result2a = new LdbcSnbBiQuery6AuthoritativeUsersResult(
                personId2,
                authorityScore2
        );
        LdbcSnbBiQuery6AuthoritativeUsersResult result3a = new LdbcSnbBiQuery6AuthoritativeUsersResult(
                personId2,
                authorityScore1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery7ResultShouldDoEqualsCorrectly()
    {
        String relatedTagName1 = "\u3055";
        int count1 = 1;

        String relatedTagName2 = "\u4e35";
        int count2 = 2;

        LdbcSnbBiQuery7RelatedTopicsResult result1a = new LdbcSnbBiQuery7RelatedTopicsResult(
                relatedTagName1,
                count1
        );
        LdbcSnbBiQuery7RelatedTopicsResult result1b = new LdbcSnbBiQuery7RelatedTopicsResult(
                relatedTagName1,
                count1
        );
        LdbcSnbBiQuery7RelatedTopicsResult result2a = new LdbcSnbBiQuery7RelatedTopicsResult(
                relatedTagName2,
                count2
        );
        LdbcSnbBiQuery7RelatedTopicsResult result3a = new LdbcSnbBiQuery7RelatedTopicsResult(
                relatedTagName2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery8ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int score1 = 2;
        int friendsScore1 = 2;

        long personId2 = 3;
        int score2 = 4;
        int friendsScore2 = 4;

        LdbcSnbBiQuery8TagPersonResult result1a = new LdbcSnbBiQuery8TagPersonResult(
                personId1,
                score1,
                friendsScore1
        );
        LdbcSnbBiQuery8TagPersonResult result1b = new LdbcSnbBiQuery8TagPersonResult(
                personId1,
                score1,
                friendsScore1
        );
        LdbcSnbBiQuery8TagPersonResult result2a = new LdbcSnbBiQuery8TagPersonResult(
                personId2,
                score2,
                friendsScore2
        );
        LdbcSnbBiQuery8TagPersonResult result3a = new LdbcSnbBiQuery8TagPersonResult(
                personId2,
                score1,
                friendsScore1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery9ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        int threadCount1 = 4;
        int messageCount1 = 5;

        long personId2 = 6;
        String personFirstName2 = "7";
        String personLastName2 = "8";
        int threadCount2 = 9;
        int messageCount2 = 10;

        LdbcSnbBiQuery9TopThreadInitiatorsResult result1a = new LdbcSnbBiQuery9TopThreadInitiatorsResult(
                personId1,
                personFirstName1,
                personLastName1,
                threadCount1,
                messageCount1
        );
        LdbcSnbBiQuery9TopThreadInitiatorsResult result1b = new LdbcSnbBiQuery9TopThreadInitiatorsResult(
                personId1,
                personFirstName1,
                personLastName1,
                threadCount1,
                messageCount1
        );
        LdbcSnbBiQuery9TopThreadInitiatorsResult result2a = new LdbcSnbBiQuery9TopThreadInitiatorsResult(
                personId2,
                personFirstName2,
                personLastName2,
                threadCount2,
                messageCount2
        );
        LdbcSnbBiQuery9TopThreadInitiatorsResult result3a = new LdbcSnbBiQuery9TopThreadInitiatorsResult(
                personId2,
                personFirstName2,
                personLastName2,
                threadCount2,
                messageCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery10ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        String tagName1 = "2";
        int messageCount1 = 3;

        long personId2 = 4;
        String tagName2 = "5";
        int messageCount2 = 6;

        LdbcSnbBiQuery10ExpertsInSocialCircleResult result1a = new LdbcSnbBiQuery10ExpertsInSocialCircleResult(
                personId1,
                tagName1,
                messageCount1
        );
        LdbcSnbBiQuery10ExpertsInSocialCircleResult result1b = new LdbcSnbBiQuery10ExpertsInSocialCircleResult(
                personId1,
                tagName1,
                messageCount1
        );
        LdbcSnbBiQuery10ExpertsInSocialCircleResult result2a = new LdbcSnbBiQuery10ExpertsInSocialCircleResult(
                personId2,
                tagName2,
                messageCount2
        );
        LdbcSnbBiQuery10ExpertsInSocialCircleResult result3a = new LdbcSnbBiQuery10ExpertsInSocialCircleResult(
                personId2,
                tagName2,
                messageCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery11ResultShouldDoEqualsCorrectly()
    {
        int count1 = 1;

        int count2 = 2;

        LdbcSnbBiQuery11FriendshipTrianglesResult result1a = new LdbcSnbBiQuery11FriendshipTrianglesResult(
                count1
        );
        LdbcSnbBiQuery11FriendshipTrianglesResult result1b = new LdbcSnbBiQuery11FriendshipTrianglesResult(
                count1
        );
        LdbcSnbBiQuery11FriendshipTrianglesResult result2a = new LdbcSnbBiQuery11FriendshipTrianglesResult(
                count2
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
    }

    @Test
    public void ldbcQuery12ResultShouldDoEqualsCorrectly()
    {
        int messageCount1 = 1;
        int personCount1 = 2;

        int messageCount2 = 3;
        int personCount2 = 4;

        LdbcSnbBiQuery12PersonPostCountsResult result1a = new LdbcSnbBiQuery12PersonPostCountsResult(
                messageCount1,
                personCount1
        );
        LdbcSnbBiQuery12PersonPostCountsResult result1b = new LdbcSnbBiQuery12PersonPostCountsResult(
                messageCount1,
                personCount1
        );
        LdbcSnbBiQuery12PersonPostCountsResult result2a = new LdbcSnbBiQuery12PersonPostCountsResult(
                messageCount2,
                personCount2
        );
        LdbcSnbBiQuery12PersonPostCountsResult result3a = new LdbcSnbBiQuery12PersonPostCountsResult(
                messageCount2,
                personCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery13ResultShouldDoEqualsCorrectly()
    {
        long zombieId1 = 1;
        int zombieLikeCount1 = 2;
        int totalLikeCount1 = 3;
        double zombieScore1 = 4.1;

        long zombieId2 = 5;
        int zombieLikeCount2 = 6;
        int totalLikeCount2 = 7;
        double zombieScore2 = 8.0;

        LdbcSnbBiQuery13ZombiesResult result1a = new LdbcSnbBiQuery13ZombiesResult(
                zombieId1,
                zombieLikeCount1,
                totalLikeCount1,
                zombieScore1
        );
        LdbcSnbBiQuery13ZombiesResult result1b = new LdbcSnbBiQuery13ZombiesResult(
                zombieId1,
                zombieLikeCount1,
                totalLikeCount1,
                zombieScore1
        );
        LdbcSnbBiQuery13ZombiesResult result2a = new LdbcSnbBiQuery13ZombiesResult(
                zombieId2,
                zombieLikeCount2,
                totalLikeCount2,
                zombieScore2
        );
        LdbcSnbBiQuery13ZombiesResult result3a = new LdbcSnbBiQuery13ZombiesResult(
                zombieId2,
                zombieLikeCount2,
                totalLikeCount2,
                zombieScore1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery14ResultShouldDoEqualsCorrectly()
    {
        long personId11 = 1;
        long personId21 = 2;
        String city1Name1 = "Toronto";
        int score1 = 3;

        long personId12 = 4;
        long personId22 = 5;
        String city1Name2 = "Chicago";
        int score2 = 6;

        LdbcSnbBiQuery14InternationalDialogResult result1a = new LdbcSnbBiQuery14InternationalDialogResult(
                personId11,
                personId21,
                city1Name1,
                score1
        );
        LdbcSnbBiQuery14InternationalDialogResult result1b = new LdbcSnbBiQuery14InternationalDialogResult(
                personId11,
                personId21,
                city1Name1,
                score1
        );
        LdbcSnbBiQuery14InternationalDialogResult result2a = new LdbcSnbBiQuery14InternationalDialogResult(
                personId12,
                personId22,
                city1Name2,
                score2
        );
        LdbcSnbBiQuery14InternationalDialogResult result3a = new LdbcSnbBiQuery14InternationalDialogResult(
                personId12,
                personId22,
                city1Name1,
                score1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery15ResultShouldDoEqualsCorrectly()
    {
        List<Long> personIds1 = Lists.newArrayList(1L, 2L, 3L);
        List<Long> personIds2 = Lists.newArrayList(1L, 2L, 3L);
        List<Long> personIds3 = Lists.newArrayList(1L, 2L);
        double weight = 0.5;

        LdbcSnbBiQuery15WeightedPathsResult result1a = new LdbcSnbBiQuery15WeightedPathsResult( personIds1, weight );
        LdbcSnbBiQuery15WeightedPathsResult result2a = new LdbcSnbBiQuery15WeightedPathsResult( personIds2, weight );
        LdbcSnbBiQuery15WeightedPathsResult result3a = new LdbcSnbBiQuery15WeightedPathsResult( personIds3, weight );

        assertThat( result1a, equalTo( result2a ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }
}
