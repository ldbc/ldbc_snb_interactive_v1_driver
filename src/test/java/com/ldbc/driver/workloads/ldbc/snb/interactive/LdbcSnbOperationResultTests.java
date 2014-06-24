package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class LdbcSnbOperationResultTests {
    @Test
    public void ldbcQuery1ResultShouldDoEqualsCorrectly() {
        long friendId1 = 1;
        String friendLastName1 = "last1";
        int friendDistance1 = 1;
        long friendBirthday1 = 1;
        long friendCreationDate1 = 1;
        String friendGender1 = "1";
        String friendBrowserUsed1 = "1";
        String friendLocationIp1 = "1";
        Iterable<String> friendEmails1 = Lists.newArrayList("1a", "1b");
        Iterable<String> friendLanguages1 = Lists.newArrayList("1a", "1b");
        String friendCityName1 = "1";
        Iterable<String> friendUniversities1 = Lists.newArrayList("1a", "1b");
        Iterable<String> friendCompanies1 = Lists.newArrayList("1a", "1b");

        long friendId2 = 2;
        String friendLastName2 = "last2";
        int friendDistance2 = 2;
        long friendBirthday2 = 2;
        long friendCreationDate2 = 2;
        String friendGender2 = "2";
        String friendBrowserUsed2 = "2";
        String friendLocationIp2 = "2";
        Iterable<String> friendEmails2 = Lists.newArrayList("2a", "2b");
        Iterable<String> friendLanguages2 = Lists.newArrayList("2a", "2b");
        String friendCityName2 = "2";
        Iterable<String> friendUniversities2 = Lists.newArrayList("2a", "2b");
        Iterable<String> friendCompanies2 = Lists.newArrayList("2a", "2b");

        LdbcQuery1Result result1a = new LdbcQuery1Result(
                friendId1,
                friendLastName1,
                friendDistance1,
                friendBirthday1,
                friendCreationDate1,
                friendGender1,
                friendBrowserUsed1,
                friendLocationIp1,
                friendEmails1,
                friendLanguages1,
                friendCityName1,
                friendUniversities1,
                friendCompanies1
        );

        LdbcQuery1Result result1b = new LdbcQuery1Result(
                friendId1,
                friendLastName1,
                friendDistance1,
                friendBirthday1,
                friendCreationDate1,
                friendGender1,
                friendBrowserUsed1,
                friendLocationIp1,
                friendEmails1,
                friendLanguages1,
                friendCityName1,
                Lists.newArrayList("1b", "1a"),
                friendCompanies1
        );

        LdbcQuery1Result result2a = new LdbcQuery1Result(
                friendId2,
                friendLastName2,
                friendDistance2,
                friendBirthday2,
                friendCreationDate2,
                friendGender2,
                friendBrowserUsed2,
                friendLocationIp2,
                friendEmails2,
                friendLanguages2,
                friendCityName2,
                friendUniversities2,
                friendCompanies2
        );

        LdbcQuery1Result result3a = new LdbcQuery1Result(
                friendId1,
                friendLastName1,
                friendDistance1,
                friendBirthday1,
                friendCreationDate1,
                friendGender1,
                friendBrowserUsed1,
                friendLocationIp1,
                friendEmails1,
                friendLanguages1,
                friendCityName1,
                friendUniversities1,
                friendCompanies2
        );

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery2ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        long postOrCommentId1 = 1;
        String postOrCommentContent1 = "1";
        long postOrCommentCreationDate1 = 1;

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        long postOrCommentId2 = 2;
        String postOrCommentContent2 = "2";
        long postOrCommentCreationDate2 = 2;

        LdbcQuery2Result result1a = new LdbcQuery2Result(personId1, personFirstName1, personLastName1, postOrCommentId1, postOrCommentContent1, postOrCommentCreationDate1);
        LdbcQuery2Result result1b = new LdbcQuery2Result(personId1, personFirstName1, personLastName1, postOrCommentId1, postOrCommentContent1, postOrCommentCreationDate1);
        LdbcQuery2Result result2a = new LdbcQuery2Result(personId2, personFirstName2, personLastName2, postOrCommentId2, postOrCommentContent2, postOrCommentCreationDate2);
        LdbcQuery2Result result3a = new LdbcQuery2Result(personId1, personFirstName1, personLastName1, postOrCommentId1, postOrCommentContent1, postOrCommentCreationDate2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery3ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        int xCount1 = 1;
        int yCount1 = 2;
        int count1 = 3;

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        int xCount2 = 3;
        int yCount2 = 4;
        int count2 = 7;

        LdbcQuery3Result result1a = new LdbcQuery3Result(personId1, personFirstName1, personLastName1, xCount1, yCount1, count1);
        LdbcQuery3Result result1b = new LdbcQuery3Result(personId1, personFirstName1, personLastName1, xCount1, yCount1, count1);
        LdbcQuery3Result result2a = new LdbcQuery3Result(personId2, personFirstName2, personLastName2, xCount2, yCount2, count2);
        LdbcQuery3Result result3a = new LdbcQuery3Result(personId1, personFirstName1, personLastName1, xCount1, yCount1, count2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery4ResultShouldDoEqualsCorrectly() {
        String tagName1 = "1";
        int tagCount1 = 1;

        String tagName2 = "2";
        int tagCount2 = 2;

        LdbcQuery4Result result1a = new LdbcQuery4Result(tagName1, tagCount1);
        LdbcQuery4Result result1b = new LdbcQuery4Result(tagName1, tagCount1);
        LdbcQuery4Result result2a = new LdbcQuery4Result(tagName2, tagCount2);
        LdbcQuery4Result result3a = new LdbcQuery4Result(tagName1, tagCount2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery5ResultShouldDoEqualsCorrectly() {
        String forumTitle1 = "1";
        int postCount1 = 1;

        String forumTitle2 = "2";
        int postCount2 = 2;

        LdbcQuery5Result result1a = new LdbcQuery5Result(forumTitle1, postCount1);
        LdbcQuery5Result result1b = new LdbcQuery5Result(forumTitle1, postCount1);
        LdbcQuery5Result result2a = new LdbcQuery5Result(forumTitle2, postCount2);
        LdbcQuery5Result result3a = new LdbcQuery5Result(forumTitle1, postCount2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery6ResultShouldDoEqualsCorrectly() {
        String tagName1 = "1";
        int tagCount1 = 1;

        String tagName2 = "2";
        int tagCount2 = 2;

        LdbcQuery6Result result1a = new LdbcQuery6Result(tagName1, tagCount1);
        LdbcQuery6Result result1b = new LdbcQuery6Result(tagName1, tagCount1);
        LdbcQuery6Result result2a = new LdbcQuery6Result(tagName2, tagCount2);
        LdbcQuery6Result result3a = new LdbcQuery6Result(tagName1, tagCount2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery7ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        long likeCreationDate1 = 1;
        long commentOrPostId1 = 1;
        String commentOrPostContent1 = "1";
        int minutesLatency1 = 1;
        boolean isNew1 = true;

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        long likeCreationDate2 = 2;
        long commentOrPostId2 = 2;
        String commentOrPostContent2 = "2";
        int minutesLatency2 = 2;
        boolean isNew2 = false;

        LdbcQuery7Result result1a = new LdbcQuery7Result(personId1, personFirstName1, personLastName1, likeCreationDate1, commentOrPostId1, commentOrPostContent1, minutesLatency1, isNew1);
        LdbcQuery7Result result1b = new LdbcQuery7Result(personId1, personFirstName1, personLastName1, likeCreationDate1, commentOrPostId1, commentOrPostContent1, minutesLatency1, isNew1);
        LdbcQuery7Result result2a = new LdbcQuery7Result(personId2, personFirstName2, personLastName2, likeCreationDate2, commentOrPostId2, commentOrPostContent2, minutesLatency2, isNew2);
        LdbcQuery7Result result3a = new LdbcQuery7Result(personId1, personFirstName1, personLastName1, likeCreationDate1, commentOrPostId1, commentOrPostContent1, minutesLatency1, isNew2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery8ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        long commentCreationDate1 = 1;
        long commentId1 = 1;
        String commentContent1 = "1";

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        long commentCreationDate2 = 2;
        long commentId2 = 2;
        String commentContent2 = "2";

        LdbcQuery8Result result1a = new LdbcQuery8Result(personId1, personFirstName1, personLastName1, commentCreationDate1, commentId1, commentContent1);
        LdbcQuery8Result result1b = new LdbcQuery8Result(personId1, personFirstName1, personLastName1, commentCreationDate1, commentId1, commentContent1);
        LdbcQuery8Result result2a = new LdbcQuery8Result(personId2, personFirstName2, personLastName2, commentCreationDate2, commentId2, commentContent2);
        LdbcQuery8Result result3a = new LdbcQuery8Result(personId1, personFirstName1, personLastName1, commentCreationDate1, commentId1, commentContent2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery9ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        long commentOrPostId1 = 1;
        String commentOrPostContent1 = "1";
        long commentOrPostCreationDate1 = 1;

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        long commentOrPostId2 = 2;
        String commentOrPostContent2 = "2";
        long commentOrPostCreationDate2 = 2;

        LdbcQuery9Result result1a = new LdbcQuery9Result(personId1, personFirstName1, personLastName1, commentOrPostId1, commentOrPostContent1, commentOrPostCreationDate1);
        LdbcQuery9Result result1b = new LdbcQuery9Result(personId1, personFirstName1, personLastName1, commentOrPostId1, commentOrPostContent1, commentOrPostCreationDate1);
        LdbcQuery9Result result2a = new LdbcQuery9Result(personId2, personFirstName2, personLastName2, commentOrPostId2, commentOrPostContent2, commentOrPostCreationDate2);
        LdbcQuery9Result result3a = new LdbcQuery9Result(personId1, personFirstName1, personLastName1, commentOrPostId1, commentOrPostContent1, commentOrPostCreationDate2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery10ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        double commonInterestScore1 = 1d;
        String personGender1 = "1";
        String personCityName1 = "1";

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        double commonInterestScore2 = 2d;
        String personGender2 = "2";
        String personCityName2 = "2";

        LdbcQuery10Result result1a = new LdbcQuery10Result(personId1, personFirstName1, personLastName1, commonInterestScore1, personGender1, personCityName1);
        LdbcQuery10Result result1b = new LdbcQuery10Result(personId1, personFirstName1, personLastName1, commonInterestScore1, personGender1, personCityName1);
        LdbcQuery10Result result2a = new LdbcQuery10Result(personId2, personFirstName2, personLastName2, commonInterestScore2, personGender2, personCityName2);
        LdbcQuery10Result result3a = new LdbcQuery10Result(personId1, personFirstName1, personLastName1, commonInterestScore1, personGender1, personCityName2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery11ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        String organizationName1 = "1";
        int organizationWorkFromYear1 = 1;

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        String organizationName2 = "2";
        int organizationWorkFromYear2 = 2;

        LdbcQuery11Result result1a = new LdbcQuery11Result(personId1, personFirstName1, personLastName1, organizationName1, organizationWorkFromYear1);
        LdbcQuery11Result result1b = new LdbcQuery11Result(personId1, personFirstName1, personLastName1, organizationName1, organizationWorkFromYear1);
        LdbcQuery11Result result2a = new LdbcQuery11Result(personId2, personFirstName2, personLastName2, organizationName2, organizationWorkFromYear2);
        LdbcQuery11Result result3a = new LdbcQuery11Result(personId1, personFirstName1, personLastName1, organizationName1, organizationWorkFromYear2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery12ResultShouldDoEqualsCorrectly() {
        long personId1 = 1;
        String personFirstName1 = "1";
        String personLastName1 = "1";
        Iterable<String> tagNames1 = Lists.newArrayList("1", "2");
        int replyCount1 = 1;

        long personId2 = 2;
        String personFirstName2 = "2";
        String personLastName2 = "2";
        Iterable<String> tagNames2 = Lists.newArrayList("3", "4");
        int replyCount2 = 2;

        LdbcQuery12Result result1a = new LdbcQuery12Result(personId1, personFirstName1, personLastName1, tagNames1, replyCount1);
        LdbcQuery12Result result1b = new LdbcQuery12Result(personId1, personFirstName1, personLastName1, Lists.newArrayList("2", "1"), replyCount1);
        LdbcQuery12Result result2a = new LdbcQuery12Result(personId2, personFirstName2, personLastName2, tagNames2, replyCount2);
        LdbcQuery12Result result3a = new LdbcQuery12Result(personId1, personFirstName1, personLastName1, tagNames1, replyCount2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result3a)));
    }

    @Test
    public void ldbcQuery13ResultShouldDoEqualsCorrectly() {
        int shortestPathLength1 = 1;

        int shortestPathLength2 = 2;

        LdbcQuery13Result result1a = new LdbcQuery13Result(shortestPathLength1);
        LdbcQuery13Result result1b = new LdbcQuery13Result(shortestPathLength1);
        LdbcQuery13Result result2a = new LdbcQuery13Result(shortestPathLength2);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1b, not(equalTo(result2a)));
    }

    @Test
    public void ldbcQuery14ResultShouldDoEqualsCorrectly() {
        LdbcQuery14Result result1a = new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 1d);
        LdbcQuery14Result result1b = new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 1d);
        LdbcQuery14Result result2a = new LdbcQuery14Result(Lists.newArrayList(1l, 3l), 2d);
        LdbcQuery14Result result3a = new LdbcQuery14Result(Lists.newArrayList(2l, 1l), 1d);
        LdbcQuery14Result result4a = new LdbcQuery14Result(Lists.newArrayList(1l, 2l), 2d);

        assertThat(result1a, equalTo(result1b));
        assertThat(result1a, not(equalTo(result2a)));
        assertThat(result1a, not(equalTo(result3a)));
        assertThat(result1a, not(equalTo(result4a)));
        assertThat(result2a, not(equalTo(result3a)));
        assertThat(result2a, not(equalTo(result4a)));
        assertThat(result3a, not(equalTo(result4a)));
    }

}