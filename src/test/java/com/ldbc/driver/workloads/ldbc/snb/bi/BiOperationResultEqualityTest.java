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
        String countryName1 = "\u3055";
        int messageMonth1 = 1;
        String personGender1 = "personGender1";
        int ageGroup1 = 2;
        String tagName1 = "\u4e35";
        int messageCount1 = 3;

        String countryName2 = "\u16a0";
        int messageMonth2 = 4;
        String personGender2 = "personGender2";
        int ageGroup2 = 5;
        String tagName2 = "tag";
        int messageCount2 = 6;

        LdbcSnbBiQuery2TopTagsResult result1a = new LdbcSnbBiQuery2TopTagsResult(
                countryName1,
                messageMonth1,
                personGender1,
                ageGroup1,
                tagName1,
                messageCount1
        );
        LdbcSnbBiQuery2TopTagsResult result1b = new LdbcSnbBiQuery2TopTagsResult(
                countryName1,
                messageMonth1,
                personGender1,
                ageGroup1,
                tagName1,
                messageCount1
        );
        LdbcSnbBiQuery2TopTagsResult result2a = new LdbcSnbBiQuery2TopTagsResult(
                countryName2,
                messageMonth2,
                personGender2,
                ageGroup2,
                tagName2,
                messageCount2
        );
        LdbcSnbBiQuery2TopTagsResult result3a = new LdbcSnbBiQuery2TopTagsResult(
                countryName2,
                messageMonth2,
                personGender2,
                ageGroup2,
                tagName2,
                messageCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery3ResultShouldDoEqualsCorrectly()
    {
        String tagName1 = "\u3055";
        int countMonth11 = 1;
        int countMonth21 = 2;
        int diff1 = 3;

        String tagName2 = "\u4e35";
        int countMonth12 = 4;
        int countMonth22 = 5;
        int diff2 = 6;

        LdbcSnbBiQuery3TagEvolutionResult result1a = new LdbcSnbBiQuery3TagEvolutionResult(
                tagName1,
                countMonth11,
                countMonth21,
                diff1
        );
        LdbcSnbBiQuery3TagEvolutionResult result1b = new LdbcSnbBiQuery3TagEvolutionResult(
                tagName1,
                countMonth11,
                countMonth21,
                diff1
        );
        LdbcSnbBiQuery3TagEvolutionResult result2a = new LdbcSnbBiQuery3TagEvolutionResult(
                tagName2,
                countMonth12,
                countMonth22,
                diff2
        );
        LdbcSnbBiQuery3TagEvolutionResult result3a = new LdbcSnbBiQuery3TagEvolutionResult(
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
    public void ldbcQuery4ResultShouldDoEqualsCorrectly()
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

        LdbcSnbBiQuery4PopularCountryTopicsResult result1a = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId1,
                forumTitle1,
                forumCreationDate1,
                personId1,
                postCount1
        );
        LdbcSnbBiQuery4PopularCountryTopicsResult result1b = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId1,
                forumTitle1,
                forumCreationDate1,
                personId1,
                postCount1
        );
        LdbcSnbBiQuery4PopularCountryTopicsResult result2a = new LdbcSnbBiQuery4PopularCountryTopicsResult(
                forumId2,
                forumTitle2,
                forumCreationDate2,
                personId2,
                postCount2
        );
        LdbcSnbBiQuery4PopularCountryTopicsResult result3a = new LdbcSnbBiQuery4PopularCountryTopicsResult(
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
    public void ldbcQuery5ResultShouldDoEqualsCorrectly()
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

        LdbcSnbBiQuery5TopCountryPostersResult result1a = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId1,
                personFirstName1,
                personLastName1,
                personCreationDate1,
                postCount1
        );
        LdbcSnbBiQuery5TopCountryPostersResult result1b = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId1,
                personFirstName1,
                personLastName1,
                personCreationDate1,
                postCount1
        );
        LdbcSnbBiQuery5TopCountryPostersResult result2a = new LdbcSnbBiQuery5TopCountryPostersResult(
                personId2,
                personFirstName2,
                personLastName2,
                personCreationDate2,
                count2
        );
        LdbcSnbBiQuery5TopCountryPostersResult result3a = new LdbcSnbBiQuery5TopCountryPostersResult(
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
    public void ldbcQuery6ResultShouldDoEqualsCorrectly()
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

        LdbcSnbBiQuery6ActivePostersResult result1a = new LdbcSnbBiQuery6ActivePostersResult(
                personId1,
                replyCount1,
                likeCount1,
                messageCount1,
                score1
        );
        LdbcSnbBiQuery6ActivePostersResult result1b = new LdbcSnbBiQuery6ActivePostersResult(
                personId1,
                replyCount1,
                likeCount1,
                messageCount1,
                score1
        );
        LdbcSnbBiQuery6ActivePostersResult result2a = new LdbcSnbBiQuery6ActivePostersResult(
                personId2,
                replyCount2,
                likeCount2,
                messageCount2,
                score2
        );
        LdbcSnbBiQuery6ActivePostersResult result3a = new LdbcSnbBiQuery6ActivePostersResult(
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
    public void ldbcQuery7ResultShouldDoEqualsCorrectly()
    {
        long personId1 = 1;
        int authorityScore1 = 2;

        long personId2 = 3;
        int authorityScore2 = 4;

        LdbcSnbBiQuery7AuthoritativeUsersResult result1a = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId1,
                authorityScore1
        );
        LdbcSnbBiQuery7AuthoritativeUsersResult result1b = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId1,
                authorityScore1
        );
        LdbcSnbBiQuery7AuthoritativeUsersResult result2a = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId2,
                authorityScore2
        );
        LdbcSnbBiQuery7AuthoritativeUsersResult result3a = new LdbcSnbBiQuery7AuthoritativeUsersResult(
                personId2,
                authorityScore1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery8ResultShouldDoEqualsCorrectly()
    {
        String relatedTagName1 = "\u3055";
        int count1 = 1;

        String relatedTagName2 = "\u4e35";
        int count2 = 2;

        LdbcSnbBiQuery8RelatedTopicsResult result1a = new LdbcSnbBiQuery8RelatedTopicsResult(
                relatedTagName1,
                count1
        );
        LdbcSnbBiQuery8RelatedTopicsResult result1b = new LdbcSnbBiQuery8RelatedTopicsResult(
                relatedTagName1,
                count1
        );
        LdbcSnbBiQuery8RelatedTopicsResult result2a = new LdbcSnbBiQuery8RelatedTopicsResult(
                relatedTagName2,
                count2
        );
        LdbcSnbBiQuery8RelatedTopicsResult result3a = new LdbcSnbBiQuery8RelatedTopicsResult(
                relatedTagName2,
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
        int count11 = 1;
        int count21 = 2;

        long forumId2 = 3;
        int count12 = 3;
        int count22 = 4;

        LdbcSnbBiQuery9RelatedForumsResult result1a = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId1,
                count11,
                count21
        );
        LdbcSnbBiQuery9RelatedForumsResult result1b = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId1,
                count11,
                count21
        );
        LdbcSnbBiQuery9RelatedForumsResult result2a = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId2,
                count12,
                count22
        );
        LdbcSnbBiQuery9RelatedForumsResult result3a = new LdbcSnbBiQuery9RelatedForumsResult(
                forumId2,
                count12,
                count21
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
        String tagName1 = "\u3055";
        int likeCount1 = 2;
        int replyCount1 = 3;

        long personId2 = 4;
        String tagName2 = "\u4e35";
        int likeCount2 = 5;
        int replyCount2 = 6;

        LdbcSnbBiQuery11UnrelatedRepliesResult result1a = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId1,
                tagName1,
                likeCount1,
                replyCount1
        );
        LdbcSnbBiQuery11UnrelatedRepliesResult result1b = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId1,
                tagName1,
                likeCount1,
                replyCount1
        );
        LdbcSnbBiQuery11UnrelatedRepliesResult result2a = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId2,
                tagName2,
                likeCount2,
                replyCount2
        );
        LdbcSnbBiQuery11UnrelatedRepliesResult result3a = new LdbcSnbBiQuery11UnrelatedRepliesResult(
                personId2,
                tagName2,
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
        long messageId1 = 1;
        long messageCreationDate1 = 2;
        String creatorFirstName1 = "first1";
        String creatorLastName1 = "last1";
        int likeCount1 = 3;

        long messageId2 = 4;
        long messageCreationDate2 = 5;
        String creatorFirstName2 = "first2";
        String creatorLastName2 = "last2";
        int likeCount2 = 6;

        LdbcSnbBiQuery12TrendingPostsResult result1a = new LdbcSnbBiQuery12TrendingPostsResult(
                messageId1,
                messageCreationDate1,
                creatorFirstName1,
                creatorLastName1,
                likeCount1
        );
        LdbcSnbBiQuery12TrendingPostsResult result1b = new LdbcSnbBiQuery12TrendingPostsResult(
                messageId1,
                messageCreationDate1,
                creatorFirstName1,
                creatorLastName1,
                likeCount1
        );
        LdbcSnbBiQuery12TrendingPostsResult result2a = new LdbcSnbBiQuery12TrendingPostsResult(
                messageId2,
                messageCreationDate2,
                creatorFirstName2,
                creatorLastName2,
                likeCount2
        );
        LdbcSnbBiQuery12TrendingPostsResult result3a = new LdbcSnbBiQuery12TrendingPostsResult(
                messageId2,
                messageCreationDate2,
                creatorFirstName1,
                creatorLastName2,
                likeCount2
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
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity> popularTags1 = Lists.newArrayList(
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "a", 1 ),
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "b", 2 )
        );

        int year2 = 5;
        int month2 = 6;
        List<LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity> popularTags2 = Lists.newArrayList(
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "c", 3 ),
                new LdbcSnbBiQuery13PopularMonthlyTagsResult.TagPopularity( "d", 4 )
        );

        LdbcSnbBiQuery13PopularMonthlyTagsResult result1a = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year1,
                month1,
                popularTags1
        );
        LdbcSnbBiQuery13PopularMonthlyTagsResult result1b = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year1,
                month1,
                popularTags1
        );
        LdbcSnbBiQuery13PopularMonthlyTagsResult result2a = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year2,
                month2,
                popularTags2
        );
        LdbcSnbBiQuery13PopularMonthlyTagsResult result3a = new LdbcSnbBiQuery13PopularMonthlyTagsResult(
                year2,
                month2,
                popularTags1
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
        String personFirstName1 = "2";
        String personLastName1 = "3";
        int threadCount1 = 4;
        int messageCount1 = 5;

        long personId2 = 6;
        String personFirstName2 = "7";
        String personLastName2 = "8";
        int threadCount2 = 9;
        int messageCount2 = 10;

        LdbcSnbBiQuery14TopThreadInitiatorsResult result1a = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId1,
                personFirstName1,
                personLastName1,
                threadCount1,
                messageCount1
        );
        LdbcSnbBiQuery14TopThreadInitiatorsResult result1b = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId1,
                personFirstName1,
                personLastName1,
                threadCount1,
                messageCount1
        );
        LdbcSnbBiQuery14TopThreadInitiatorsResult result2a = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
                personId2,
                personFirstName2,
                personLastName2,
                threadCount2,
                messageCount2
        );
        LdbcSnbBiQuery14TopThreadInitiatorsResult result3a = new LdbcSnbBiQuery14TopThreadInitiatorsResult(
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
        String tagName1 = "2";
        int messageCount1 = 3;

        long personId2 = 4;
        String tagName2 = "5";
        int messageCount2 = 6;

        LdbcSnbBiQuery16ExpertsInSocialCircleResult result1a = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId1,
                tagName1,
                messageCount1
        );
        LdbcSnbBiQuery16ExpertsInSocialCircleResult result1b = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId1,
                tagName1,
                messageCount1
        );
        LdbcSnbBiQuery16ExpertsInSocialCircleResult result2a = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
                personId2,
                tagName2,
                messageCount2
        );
        LdbcSnbBiQuery16ExpertsInSocialCircleResult result3a = new LdbcSnbBiQuery16ExpertsInSocialCircleResult(
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
        int messageCount1 = 1;
        int personCount1 = 2;

        int messageCount2 = 3;
        int personCount2 = 4;

        LdbcSnbBiQuery18PersonPostCountsResult result1a = new LdbcSnbBiQuery18PersonPostCountsResult(
                messageCount1,
                personCount1
        );
        LdbcSnbBiQuery18PersonPostCountsResult result1b = new LdbcSnbBiQuery18PersonPostCountsResult(
                messageCount1,
                personCount1
        );
        LdbcSnbBiQuery18PersonPostCountsResult result2a = new LdbcSnbBiQuery18PersonPostCountsResult(
                messageCount2,
                personCount2
        );
        LdbcSnbBiQuery18PersonPostCountsResult result3a = new LdbcSnbBiQuery18PersonPostCountsResult(
                messageCount2,
                personCount1
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
        int interactionCount1 = 3;

        long personId2 = 4;
        int strangerCount2 = 5;
        int interactionCount2 = 6;

        LdbcSnbBiQuery19StrangerInteractionResult result1a = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId1,
                strangerCount1,
                interactionCount1
        );
        LdbcSnbBiQuery19StrangerInteractionResult result1b = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId1,
                strangerCount1,
                interactionCount1
        );
        LdbcSnbBiQuery19StrangerInteractionResult result2a = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId2,
                strangerCount2,
                interactionCount2
        );
        LdbcSnbBiQuery19StrangerInteractionResult result3a = new LdbcSnbBiQuery19StrangerInteractionResult(
                personId2,
                strangerCount2,
                interactionCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery20ResultShouldDoEqualsCorrectly()
    {
        String tagClassName1 = "tagClass1";
        int messageCount1 = 1;

        String tagClassName2 = "tagClass2";
        int messageCount2 = 2;

        LdbcSnbBiQuery20HighLevelTopicsResult result1a = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClassName1,
                messageCount1
        );
        LdbcSnbBiQuery20HighLevelTopicsResult result1b = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClassName1,
                messageCount1
        );
        LdbcSnbBiQuery20HighLevelTopicsResult result2a = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClassName2,
                messageCount2
        );
        LdbcSnbBiQuery20HighLevelTopicsResult result3a = new LdbcSnbBiQuery20HighLevelTopicsResult(
                tagClassName2,
                messageCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery21ResultShouldDoEqualsCorrectly()
    {
        long zombieId1 = 1;
        int zombieLikeCount1 = 2;
        int totalLikeCount1 = 3;
        double zombieScore1 = 4.1;

        long zombieId2 = 5;
        int zombieLikeCount2 = 6;
        int totalLikeCount2 = 7;
        double zombieScore2 = 8.0;

        LdbcSnbBiQuery21ZombiesResult result1a = new LdbcSnbBiQuery21ZombiesResult(
                zombieId1,
                zombieLikeCount1,
                totalLikeCount1,
                zombieScore1
        );
        LdbcSnbBiQuery21ZombiesResult result1b = new LdbcSnbBiQuery21ZombiesResult(
                zombieId1,
                zombieLikeCount1,
                totalLikeCount1,
                zombieScore1
        );
        LdbcSnbBiQuery21ZombiesResult result2a = new LdbcSnbBiQuery21ZombiesResult(
                zombieId2,
                zombieLikeCount2,
                totalLikeCount2,
                zombieScore2
        );
        LdbcSnbBiQuery21ZombiesResult result3a = new LdbcSnbBiQuery21ZombiesResult(
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
    public void ldbcQuery22ResultShouldDoEqualsCorrectly()
    {
        long personId11 = 1;
        long personId21 = 2;
        String city1Name1 = "Toronto";
        int score1 = 3;

        long personId12 = 4;
        long personId22 = 5;
        String city1Name2 = "Chicago";
        int score2 = 6;

        LdbcSnbBiQuery22InternationalDialogResult result1a = new LdbcSnbBiQuery22InternationalDialogResult(
                personId11,
                personId21,
                city1Name1,
                score1
        );
        LdbcSnbBiQuery22InternationalDialogResult result1b = new LdbcSnbBiQuery22InternationalDialogResult(
                personId11,
                personId21,
                city1Name1,
                score1
        );
        LdbcSnbBiQuery22InternationalDialogResult result2a = new LdbcSnbBiQuery22InternationalDialogResult(
                personId12,
                personId22,
                city1Name2,
                score2
        );
        LdbcSnbBiQuery22InternationalDialogResult result3a = new LdbcSnbBiQuery22InternationalDialogResult(
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
    public void ldbcQuery23ResultShouldDoEqualsCorrectly()
    {
        int messageCount1 = 1;
        String destinationName1 = "2";
        int month1 = 3;

        int messageCount2 = 4;
        String destinationName2 = "5";
        int month2 = 6;

        LdbcSnbBiQuery23HolidayDestinationsResult result1a = new LdbcSnbBiQuery23HolidayDestinationsResult(
                messageCount1,
                destinationName1,
                month1
        );
        LdbcSnbBiQuery23HolidayDestinationsResult result1b = new LdbcSnbBiQuery23HolidayDestinationsResult(
                messageCount1,
                destinationName1,
                month1
        );
        LdbcSnbBiQuery23HolidayDestinationsResult result2a = new LdbcSnbBiQuery23HolidayDestinationsResult(
                messageCount2,
                destinationName2,
                month2
        );
        LdbcSnbBiQuery23HolidayDestinationsResult result3a = new LdbcSnbBiQuery23HolidayDestinationsResult(
                messageCount1,
                destinationName2,
                month2
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
        String continentName1 = "5";

        int messageCount2 = 7;
        int likeCount2 = 8;
        int year2 = 9;
        int month2 = 10;
        String continentName2 = "11";

        LdbcSnbBiQuery24MessagesByTopicResult result1a = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount1,
                likeCount1,
                year1,
                month1,
                continentName1
        );
        LdbcSnbBiQuery24MessagesByTopicResult result1b = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount1,
                likeCount1,
                year1,
                month1,
                continentName1
        );
        LdbcSnbBiQuery24MessagesByTopicResult result2a = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount2,
                likeCount2,
                year2,
                month2,
                continentName2
        );
        LdbcSnbBiQuery24MessagesByTopicResult result3a = new LdbcSnbBiQuery24MessagesByTopicResult(
                messageCount1,
                likeCount1,
                year1,
                month1,
                continentName2
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }

    @Test
    public void ldbcQuery25ResultShouldDoEqualsCorrectly()
    {
        List<Long> personIds1 = Lists.newArrayList(1L, 2L, 3L);
        List<Long> personIds2 = Lists.newArrayList(1L, 2L, 3L);
        List<Long> personIds3 = Lists.newArrayList(1L, 2L);
        double weight = 0.5;

        LdbcSnbBiQuery25WeightedPathsResult result1a = new LdbcSnbBiQuery25WeightedPathsResult( personIds1, weight );
        LdbcSnbBiQuery25WeightedPathsResult result2a = new LdbcSnbBiQuery25WeightedPathsResult( personIds2, weight );
        LdbcSnbBiQuery25WeightedPathsResult result3a = new LdbcSnbBiQuery25WeightedPathsResult( personIds3, weight );

        assertThat( result1a, equalTo( result2a ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }
}
