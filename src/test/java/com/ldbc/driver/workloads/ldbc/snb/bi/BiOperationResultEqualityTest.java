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
        int year1 = 1;
        boolean isReply1 = false;
        int size1 = 2;
        int count1 = 3;
        int averageLength1 = 4;
        int total1 = 5;
        float percent1 = 6;

        int year2 = 7;
        boolean isReply2 = true;
        int size2 = 8;
        int count2 = 9;
        int averageLength2 = 10;
        int total2 = 11;
        float percent2 = 12;


        LdbcSnbBiQuery1PostingSummaryResult result1a = new LdbcSnbBiQuery1PostingSummaryResult(
                year1,
                isReply1,
                size1,
                count1,
                averageLength1,
                total1,
                percent1
        );
        LdbcSnbBiQuery1PostingSummaryResult result1b = new LdbcSnbBiQuery1PostingSummaryResult(
                year1,
                isReply1,
                size1,
                count1,
                averageLength1,
                total1,
                percent1
        );
        LdbcSnbBiQuery1PostingSummaryResult result2a = new LdbcSnbBiQuery1PostingSummaryResult(
                year2,
                isReply2,
                size2,
                count2,
                averageLength2,
                total2,
                percent2
        );
        LdbcSnbBiQuery1PostingSummaryResult result3a = new LdbcSnbBiQuery1PostingSummaryResult(
                year2,
                isReply2,
                size2,
                count2,
                averageLength2,
                total2,
                percent1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery2ResultShouldDoEqualsCorrectly()
    {
        String country1 = "\u3055";
        int month1 = 1;
        String gender1 = "gender1";
        int ageGroup1 = 2;
        String tag1 = "\u4e35";
        int count1 = 3;

        String country2 = "\u16a0";
        int month2 = 4;
        String gender2 = "gender2";
        int ageGroup2 = 5;
        String tag2 = "tag";
        int count2 = 6;

        LdbcSnbBiQuery2TopTagsResult result1a = new LdbcSnbBiQuery2TopTagsResult(
                country1,
                month1,
                gender1,
                ageGroup1,
                tag1,
                count1
        );
        LdbcSnbBiQuery2TopTagsResult result1b = new LdbcSnbBiQuery2TopTagsResult(
                country1,
                month1,
                gender1,
                ageGroup1,
                tag1,
                count1
        );
        LdbcSnbBiQuery2TopTagsResult result2a = new LdbcSnbBiQuery2TopTagsResult(
                country2,
                month2,
                gender2,
                ageGroup2,
                tag2,
                count2
        );
        LdbcSnbBiQuery2TopTagsResult result3a = new LdbcSnbBiQuery2TopTagsResult(
                country2,
                month2,
                gender2,
                ageGroup2,
                tag2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery3ResultShouldDoEqualsCorrectly()
    {
        String tag1 = "\u3055";
        int countA1 = 1;
        int countB1 = 2;
        int difference1 = 3;

        String tag2 = "\u4e35";
        int countA2 = 4;
        int countB2 = 5;
        int difference2 = 6;

        LdbcSnbBiQuery3TagEvolutionResult result1a = new LdbcSnbBiQuery3TagEvolutionResult(
                tag1,
                countA1,
                countB1,
                difference1
        );
        LdbcSnbBiQuery3TagEvolutionResult result1b = new LdbcSnbBiQuery3TagEvolutionResult(
                tag1,
                countA1,
                countB1,
                difference1
        );
        LdbcSnbBiQuery3TagEvolutionResult result2a = new LdbcSnbBiQuery3TagEvolutionResult(
                tag2,
                countA2,
                countB2,
                difference2
        );
        LdbcSnbBiQuery3TagEvolutionResult result3a = new LdbcSnbBiQuery3TagEvolutionResult(
                tag2,
                countA2,
                countB2,
                difference1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery4ResultShouldDoEqualsCorrectly()
    {
        long forumId1 = Long.MAX_VALUE;
        String title1 = "\u3055";
        long creationDate1 = Long.MIN_VALUE;
        long moderator1 = 1;
        int count1 = 2;

        long forumId2 = 3;
        String title2 = "\u4e35";
        long creationDate2 = 4;
        long moderator2 = 5;
        int count2 = 6;

        LdbcSnbBiQuery4PopularCountryTopicsResult result1a = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId1,
                title1,
                creationDate1,
                moderator1,
                count1
        );
        LdbcSnbBiQuery4PopularCountryTopicsResult result1b = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId1,
                title1,
                creationDate1,
                moderator1,
                count1
        );
        LdbcSnbBiQuery4PopularCountryTopicsResult result2a = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId2,
                title2,
                creationDate2,
                moderator2,
                count2
        );
        LdbcSnbBiQuery4PopularCountryTopicsResult result3a = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId2,
                title2,
                creationDate2,
                moderator2,
                count1
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
        String firstName1 = "\u3055";
        String lastName1 = "\u4e35";
        long createDate1 = 2;
        int count1 = Integer.MAX_VALUE;

        long personId2 = 3;
        String firstName2 = "4";
        String lastName2 = "5";
        long createDate2 = 6;
        int count2 = Integer.MIN_VALUE;

        LdbcSnbBiQuery5TopCountryPostersResult result1a = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery5TopCountryPostersResult result1b = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery5TopCountryPostersResult result2a = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId2,
                firstName2,
                lastName2,
                createDate2,
                count2
        );
        LdbcSnbBiQuery5TopCountryPostersResult result3a = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId2,
                firstName2,
                lastName2,
                createDate2,
                count1
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
        int postCount1 = 2;
        int replyCount1 = 3;
        int likeCount1 = 4;
        int score1 = 5;

        long personId2 = 6;
        int postCount2 = 7;
        int replyCount2 = 8;
        int likeCount2 = 9;
        int score2 = 10;

        LdbcSnbBiQuery6ActivePostersResult result1a = new LdbcSnbBiQuery6ActivePostersResult(
                personId1,
                postCount1,
                replyCount1,
                likeCount1,
                score1
        );
        LdbcSnbBiQuery6ActivePostersResult result1b = new LdbcSnbBiQuery6ActivePostersResult(
                personId1,
                postCount1,
                replyCount1,
                likeCount1,
                score1
        );
        LdbcSnbBiQuery6ActivePostersResult result2a = new LdbcSnbBiQuery6ActivePostersResult(
                personId2,
                postCount2,
                replyCount2,
                likeCount2,
                score2
        );
        LdbcSnbBiQuery6ActivePostersResult result3a = new LdbcSnbBiQuery6ActivePostersResult(
                personId2,
                postCount2,
                replyCount2,
                likeCount2,
                score1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery7ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int score1 = 2;

        long personId2 = 3;
        int score2 = 4;

        LdbcSnbBiQuery7AuthoritativeUsersResult result1a = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId1,
                score1
        );
        LdbcSnbBiQuery7AuthoritativeUsersResult result1b = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId1,
                score1
        );
        LdbcSnbBiQuery7AuthoritativeUsersResult result2a = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId2,
                score2
        );
        LdbcSnbBiQuery7AuthoritativeUsersResult result3a = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId2,
                score1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery8ResultShouldDoEqualsCorrectly()
    {
        String tag1 = "\u3055";
        int count1 = 1;

        String tag2 = "\u4e35";
        int count2 = 2;

        LdbcSnbBiQuery8RelatedTopicsResult result1a = new LdbcSnbBiQuery8RelatedTopicsResult(
                tag1,
                count1
        );
        LdbcSnbBiQuery8RelatedTopicsResult result1b = new LdbcSnbBiQuery8RelatedTopicsResult(
                tag1,
                count1
        );
        LdbcSnbBiQuery8RelatedTopicsResult result2a = new LdbcSnbBiQuery8RelatedTopicsResult(
                tag2,
                count2
        );
        LdbcSnbBiQuery8RelatedTopicsResult result3a = new LdbcSnbBiQuery8RelatedTopicsResult(
                tag2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery9ResultShouldDoEqualsCorrectly()
    {
        long forumId1 = 1;
        int sumA1 = 1;
        int sumB1 = 2;

        long forumId2 = 3;
        int sumA2 = 3;
        int sumB2 = 4;

        LdbcSnbBiQuery9RelatedForumsResult result1a = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId1,
                sumA1,
                sumB1
        );
        LdbcSnbBiQuery9RelatedForumsResult result1b = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId1,
                sumA1,
                sumB1
        );
        LdbcSnbBiQuery9RelatedForumsResult result2a = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId2,
                sumA2,
                sumB2
        );
        LdbcSnbBiQuery9RelatedForumsResult result3a = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId2,
                sumA2,
                sumB1
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
        int score1 = 2;
        int friendsScore1 = 2;

        long personId2 = 3;
        int score2 = 4;
        int friendsScore2 = 4;

        LdbcSnbBiQuery10TagPersonResult result1a = new LdbcSnbBiQuery10TagPersonResult(
                personId1,
                score1,
                friendsScore1
        );
        LdbcSnbBiQuery10TagPersonResult result1b = new LdbcSnbBiQuery10TagPersonResult(
                personId1,
                score1,
                friendsScore1
        );
        LdbcSnbBiQuery10TagPersonResult result2a = new LdbcSnbBiQuery10TagPersonResult(
                personId2,
                score2,
                friendsScore2
        );
        LdbcSnbBiQuery10TagPersonResult result3a = new LdbcSnbBiQuery10TagPersonResult(
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
    public void ldbcQuery11ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        String tag1 = "\u3055";
        int likeCount1 = 2;
        int replyCount1 = 3;

        long personId2 = 4;
        String tag2 = "\u4e35";
        int likeCount2 = 5;
        int replyCount2 = 6;

        LdbcSnbBiQuery11UnrelatedRepliesResult result1a = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId1,
                tag1,
                likeCount1,
                replyCount1
        );
        LdbcSnbBiQuery11UnrelatedRepliesResult result1b = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId1,
                tag1,
                likeCount1,
                replyCount1
        );
        LdbcSnbBiQuery11UnrelatedRepliesResult result2a = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId2,
                tag2,
                likeCount2,
                replyCount2
        );
        LdbcSnbBiQuery11UnrelatedRepliesResult result3a = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId2,
                tag2,
                likeCount2,
                replyCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery12ResultShouldDoEqualsCorrectly()
    {
        long postId1 = 1;
        String firstName1 = "first1";
        String lastName1 = "last1";
        long createDate1 = 2;
        int count1 = 3;

        long postId2 = 4;
        String firstName2 = "first2";
        String lastName2 = "last2";
        long createDate2 = 5;
        int count2 = 6;

        LdbcSnbBiQuery12TrendingPostsResult result1a = new LdbcSnbBiQuery12TrendingPostsResult(
                postId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery12TrendingPostsResult result1b = new LdbcSnbBiQuery12TrendingPostsResult(
                postId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery12TrendingPostsResult result2a = new LdbcSnbBiQuery12TrendingPostsResult(
                postId2,
                firstName2,
                lastName2,
                createDate2,
                count2
        );
        LdbcSnbBiQuery12TrendingPostsResult result3a = new LdbcSnbBiQuery12TrendingPostsResult(
                postId2,
                firstName1,
                lastName2,
                createDate2,
                count2
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery13ResultShouldDoEqualsCorrectly()
    {
        int year1 = 1;
        int month1 = 2;
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity> tagPopularities1 = Lists.newArrayList(
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "a", 1 ),
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "b", 2 )
        );

        int year2 = 5;
        int month2 = 6;
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity> tagPopularities2 = Lists.newArrayList(
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "c", 3 ),
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "d", 4 )
        );

        LdbcSnbBiQuery13PopularMonthlyTagsResult result1a = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year1,
                month1,
                tagPopularities1
        );
        LdbcSnbBiQuery13PopularMonthlyTagsResult result1b = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year1,
                month1,
                tagPopularities1
        );
        LdbcSnbBiQuery13PopularMonthlyTagsResult result2a = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year2,
                month2,
                tagPopularities2
        );
        LdbcSnbBiQuery13PopularMonthlyTagsResult result3a = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year2,
                month2,
                tagPopularities1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery14ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        String firstName1 = "2";
        String lastName1 = "3";
        int count1 = 4;
        int threadCount1 = 5;

        long personId2 = 6;
        String firstName2 = "7";
        String lastName2 = "8";
        int count2 = 9;
        int threadCount2 = 10;

        LdbcSnbBiQuery14TopThreadInitiatorsResult result1a = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId1,
                firstName1,
                lastName1,
                count1,
                threadCount1
        );
        LdbcSnbBiQuery14TopThreadInitiatorsResult result1b = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId1,
                firstName1,
                lastName1,
                count1,
                threadCount1
        );
        LdbcSnbBiQuery14TopThreadInitiatorsResult result2a = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId2,
                firstName2,
                lastName2,
                count2,
                threadCount2
        );
        LdbcSnbBiQuery14TopThreadInitiatorsResult result3a = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId2,
                firstName2,
                lastName2,
                count2,
                threadCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery15ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int count1 = 2;

        long personId2 = 3;
        int count2 = 4;

        LdbcSnbBiQuery15SocialNormalsResult result1a = new LdbcSnbBiQuery15SocialNormalsResult(
                personId1,
                count1
        );
        LdbcSnbBiQuery15SocialNormalsResult result1b = new LdbcSnbBiQuery15SocialNormalsResult(
                personId1,
                count1
        );
        LdbcSnbBiQuery15SocialNormalsResult result2a = new LdbcSnbBiQuery15SocialNormalsResult(
                personId2,
                count2
        );
        LdbcSnbBiQuery15SocialNormalsResult result3a = new LdbcSnbBiQuery15SocialNormalsResult(
                personId2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery16ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        String tag1 = "2";
        int count1 = 3;

        long personId2 = 4;
        String tag2 = "5";
        int count2 = 6;

        LdbcSnbBiQuery16ExpertsInSocialCircleResult result1a = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId1,
                tag1,
                count1
        );
        LdbcSnbBiQuery16ExpertsInSocialCircleResult result1b = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId1,
                tag1,
                count1
        );
        LdbcSnbBiQuery16ExpertsInSocialCircleResult result2a = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId2,
                tag2,
                count2
        );
        LdbcSnbBiQuery16ExpertsInSocialCircleResult result3a = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId2,
                tag2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery17ResultShouldDoEqualsCorrectly()
    {
        int count1 = 1;

        int count2 = 2;

        LdbcSnbBiQuery17FriendshipTrianglesResult result1a = new LdbcSnbBiQuery17FriendshipTrianglesResult(
                count1
        );
        LdbcSnbBiQuery17FriendshipTrianglesResult result1b = new LdbcSnbBiQuery17FriendshipTrianglesResult(
                count1
        );
        LdbcSnbBiQuery17FriendshipTrianglesResult result2a = new LdbcSnbBiQuery17FriendshipTrianglesResult(
                count2
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
    }

    @Test
    public void ldbcQuery18ResultShouldDoEqualsCorrectly()
    {
        int postCount1 = 1;
        int count1 = 2;

        int postCount2 = 3;
        int count2 = 4;

        LdbcSnbBiQuery18PersonPostCountsResult result1a = new LdbcSnbBiQuery18PersonPostCountsResult(
                postCount1,
                count1
        );
        LdbcSnbBiQuery18PersonPostCountsResult result1b = new LdbcSnbBiQuery18PersonPostCountsResult(
                postCount1,
                count1
        );
        LdbcSnbBiQuery18PersonPostCountsResult result2a = new LdbcSnbBiQuery18PersonPostCountsResult(
                postCount2,
                count2
        );
        LdbcSnbBiQuery18PersonPostCountsResult result3a = new LdbcSnbBiQuery18PersonPostCountsResult(
                postCount2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery19ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int strangerCount1 = 2;
        int count1 = 3;

        long personId2 = 4;
        int strangerCount2 = 5;
        int count2 = 6;

        LdbcSnbBiQuery19StrangerInteractionResult result1a = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId1,
                strangerCount1,
                count1
        );
        LdbcSnbBiQuery19StrangerInteractionResult result1b = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId1,
                strangerCount1,
                count1
        );
        LdbcSnbBiQuery19StrangerInteractionResult result2a = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId2,
                strangerCount2,
                count2
        );
        LdbcSnbBiQuery19StrangerInteractionResult result3a = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId2,
                strangerCount2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery20ResultShouldDoEqualsCorrectly()
    {
        String tagClass1 = "tagClass1";
        int count1 = 1;

        String tagClass2 = "tagClass2";
        int count2 = 2;

        LdbcSnbBiQuery20HighLevelTopicsResult result1a = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClass1,
                count1
        );
        LdbcSnbBiQuery20HighLevelTopicsResult result1b = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClass1,
                count1
        );
        LdbcSnbBiQuery20HighLevelTopicsResult result2a = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClass2,
                count2
        );
        LdbcSnbBiQuery20HighLevelTopicsResult result3a = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClass2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery21ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int zombieCount1 = 2;
        int realCount1 = 3;
        double score1 = 4.1;

        long personId2 = 5;
        int zombieCount2 = 6;
        int realCount2 = 7;
        double score2 = 8.0;

        LdbcSnbBiQuery21ZombiesResult result1a = new LdbcSnbBiQuery21ZombiesResult(
                personId1,
                zombieCount1,
                realCount1,
                score1
        );
        LdbcSnbBiQuery21ZombiesResult result1b = new LdbcSnbBiQuery21ZombiesResult(
                personId1,
                zombieCount1,
                realCount1,
                score1
        );
        LdbcSnbBiQuery21ZombiesResult result2a = new LdbcSnbBiQuery21ZombiesResult(
                personId2,
                zombieCount2,
                realCount2,
                score2
        );
        LdbcSnbBiQuery21ZombiesResult result3a = new LdbcSnbBiQuery21ZombiesResult(
                personId2,
                zombieCount2,
                realCount2,
                score1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery22ResultShouldDoEqualsCorrectly()
    {
        long personId1A = 1;
        long personId2A = 2;
        int scoreA = 3;

        long personId1B = 4;
        long personId2B = 5;
        int scoreB = 6;

        LdbcSnbBiQuery22InternationalDialogResult result1a = new LdbcSnbBiQuery22InternationalDialogResult(
                personId1A,
                personId2A,
                scoreA
        );
        LdbcSnbBiQuery22InternationalDialogResult result1b = new LdbcSnbBiQuery22InternationalDialogResult(
                personId1A,
                personId2A,
                scoreA
        );
        LdbcSnbBiQuery22InternationalDialogResult result2a = new LdbcSnbBiQuery22InternationalDialogResult(
                personId1B,
                personId2B,
                scoreB
        );
        LdbcSnbBiQuery22InternationalDialogResult result3a = new LdbcSnbBiQuery22InternationalDialogResult(
                personId1B,
                personId2B,
                scoreA
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery23ResultShouldDoEqualsCorrectly()
    {
        String place1 = "1";
        int month1 = 2;
        int count1 = 3;

        String place2 = "4";
        int month2 = 5;
        int count2 = 6;

        LdbcSnbBiQuery23HolidayDestinationsResult result1a = new LdbcSnbBiQuery23HolidayDestinationsResult(
                place1,
                month1,
                count1
        );
        LdbcSnbBiQuery23HolidayDestinationsResult result1b = new LdbcSnbBiQuery23HolidayDestinationsResult(
                place1,
                month1,
                count1
        );
        LdbcSnbBiQuery23HolidayDestinationsResult result2a = new LdbcSnbBiQuery23HolidayDestinationsResult(
                place2,
                month2,
                count2
        );
        LdbcSnbBiQuery23HolidayDestinationsResult result3a = new LdbcSnbBiQuery23HolidayDestinationsResult(
                place2,
                month2,
                count1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery24ResultShouldDoEqualsCorrectly()
    {
        int messageCount1 = 1;
        int likeCount1 = 2;
        int year1 = 3;
        int month1 = 4;
        String continent1 = "5";

        int messageCount2 = 7;
        int likeCount2 = 8;
        int year2 = 9;
        int month2 = 10;
        String continent2 = "11";

        LdbcSnbBiQuery24MessagesByTopicResult result1a = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount1,
                likeCount1,
                year1,
                month1,
                continent1
        );
        LdbcSnbBiQuery24MessagesByTopicResult result1b = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount1,
                likeCount1,
                year1,
                month1,
                continent1
        );
        LdbcSnbBiQuery24MessagesByTopicResult result2a = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount2,
                likeCount2,
                year2,
                month2,
                continent2
        );
        LdbcSnbBiQuery24MessagesByTopicResult result3a = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount1,
                likeCount1,
                year1,
                month1,
                continent2
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }
}