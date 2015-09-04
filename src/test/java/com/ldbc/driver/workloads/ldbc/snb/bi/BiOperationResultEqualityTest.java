package com.ldbc.driver.workloads.ldbc.snb.bi;

import org.junit.Test;

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
        double percent1 = 6;

        int year2 = 7;
        boolean isReply2 = true;
        int size2 = 8;
        int count2 = 9;
        int averageLength2 = 10;
        int total2 = 11;
        double percent2 = 12;


        LdbcSnbBiQuery1Result result1a = new LdbcSnbBiQuery1Result(
                year1,
                isReply1,
                size1,
                count1,
                averageLength1,
                total1,
                percent1
        );
        LdbcSnbBiQuery1Result result1b = new LdbcSnbBiQuery1Result(
                year1,
                isReply1,
                size1,
                count1,
                averageLength1,
                total1,
                percent1
        );
        LdbcSnbBiQuery1Result result2a = new LdbcSnbBiQuery1Result(
                year2,
                isReply2,
                size2,
                count2,
                averageLength2,
                total2,
                percent2
        );
        LdbcSnbBiQuery1Result result3a = new LdbcSnbBiQuery1Result(
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
        String country1 = "さ";
        int month1 = 1;
        String gender1 = "gender1";
        String tag1 = "丵";
        int count1 = 2;

        String country2 = "ᚠ";
        int month2 = 3;
        String gender2 = "gender2";
        String tag2 = "tag";
        int count2 = 4;

        LdbcSnbBiQuery2Result result1a = new LdbcSnbBiQuery2Result(
                country1,
                month1,
                gender1,
                tag1,
                count1
        );
        LdbcSnbBiQuery2Result result1b = new LdbcSnbBiQuery2Result(
                country1,
                month1,
                gender1,
                tag1,
                count1
        );
        LdbcSnbBiQuery2Result result2a = new LdbcSnbBiQuery2Result(
                country2,
                month2,
                gender2,
                tag2,
                count2
        );
        LdbcSnbBiQuery2Result result3a = new LdbcSnbBiQuery2Result(
                country2,
                month2,
                gender2,
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
        String tag1 = "さ";
        int countA1 = 1;
        int countB1 = 2;
        int difference1 = 3;

        String tag2 = "丵";
        int countA2 = 4;
        int countB2 = 5;
        int difference2 = 6;

        LdbcSnbBiQuery3Result result1a = new LdbcSnbBiQuery3Result(
                tag1,
                countA1,
                countB1,
                difference1
        );
        LdbcSnbBiQuery3Result result1b = new LdbcSnbBiQuery3Result(
                tag1,
                countA1,
                countB1,
                difference1
        );
        LdbcSnbBiQuery3Result result2a = new LdbcSnbBiQuery3Result(
                tag2,
                countA2,
                countB2,
                difference2
        );
        LdbcSnbBiQuery3Result result3a = new LdbcSnbBiQuery3Result(
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
        String title1 = "さ";
        long creationDate1 = Long.MIN_VALUE;
        long moderator1 = 1;
        int count1 = 2;

        long forumId2 = 3;
        String title2 = "丵";
        long creationDate2 = 4;
        long moderator2 = 5;
        int count2 = 6;

        LdbcSnbBiQuery4Result result1a = new LdbcSnbBiQuery4Result(
                forumId1,
                title1,
                creationDate1,
                moderator1,
                count1
        );
        LdbcSnbBiQuery4Result result1b = new LdbcSnbBiQuery4Result(
                forumId1,
                title1,
                creationDate1,
                moderator1,
                count1
        );
        LdbcSnbBiQuery4Result result2a = new LdbcSnbBiQuery4Result(
                forumId2,
                title2,
                creationDate2,
                moderator2,
                count2
        );
        LdbcSnbBiQuery4Result result3a = new LdbcSnbBiQuery4Result(
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
        String firstName1 = "さ";
        String lastName1 = "丵";
        long createDate1 = 2;
        int count1 = Integer.MAX_VALUE;

        long personId2 = 3;
        String firstName2 = "4";
        String lastName2 = "5";
        long createDate2 = 6;
        int count2 = Integer.MIN_VALUE;

        LdbcSnbBiQuery5Result result1a = new LdbcSnbBiQuery5Result(
                personId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery5Result result1b = new LdbcSnbBiQuery5Result(
                personId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery5Result result2a = new LdbcSnbBiQuery5Result(
                personId2,
                firstName2,
                lastName2,
                createDate2,
                count2
        );
        LdbcSnbBiQuery5Result result3a = new LdbcSnbBiQuery5Result(
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

        LdbcSnbBiQuery6Result result1a = new LdbcSnbBiQuery6Result(
                personId1,
                postCount1,
                replyCount1,
                likeCount1,
                score1
        );
        LdbcSnbBiQuery6Result result1b = new LdbcSnbBiQuery6Result(
                personId1,
                postCount1,
                replyCount1,
                likeCount1,
                score1
        );
        LdbcSnbBiQuery6Result result2a = new LdbcSnbBiQuery6Result(
                personId2,
                postCount2,
                replyCount2,
                likeCount2,
                score2
        );
        LdbcSnbBiQuery6Result result3a = new LdbcSnbBiQuery6Result(
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

        LdbcSnbBiQuery7Result result1a = new LdbcSnbBiQuery7Result(
                personId1,
                score1
        );
        LdbcSnbBiQuery7Result result1b = new LdbcSnbBiQuery7Result(
                personId1,
                score1
        );
        LdbcSnbBiQuery7Result result2a = new LdbcSnbBiQuery7Result(
                personId2,
                score2
        );
        LdbcSnbBiQuery7Result result3a = new LdbcSnbBiQuery7Result(
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
        String tag1 = "さ";
        int count1 = 1;

        String tag2 = "丵";
        int count2 = 2;

        LdbcSnbBiQuery8Result result1a = new LdbcSnbBiQuery8Result(
                tag1,
                count1
        );
        LdbcSnbBiQuery8Result result1b = new LdbcSnbBiQuery8Result(
                tag1,
                count1
        );
        LdbcSnbBiQuery8Result result2a = new LdbcSnbBiQuery8Result(
                tag2,
                count2
        );
        LdbcSnbBiQuery8Result result3a = new LdbcSnbBiQuery8Result(
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
        String forumTitle1 = "さ";
        int sumA1 = 1;
        int sumB1 = 2;

        String forumTitle2 = "丵";
        int sumA2 = 3;
        int sumB2 = 4;

        LdbcSnbBiQuery9Result result1a = new LdbcSnbBiQuery9Result(
                forumTitle1,
                sumA1,
                sumB1
        );
        LdbcSnbBiQuery9Result result1b = new LdbcSnbBiQuery9Result(
                forumTitle1,
                sumA1,
                sumB1
        );
        LdbcSnbBiQuery9Result result2a = new LdbcSnbBiQuery9Result(
                forumTitle2,
                sumA2,
                sumB2
        );
        LdbcSnbBiQuery9Result result3a = new LdbcSnbBiQuery9Result(
                forumTitle2,
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

        long personId2 = 3;
        int score2 = 4;

        LdbcSnbBiQuery10Result result1a = new LdbcSnbBiQuery10Result(
                personId1,
                score1
        );
        LdbcSnbBiQuery10Result result1b = new LdbcSnbBiQuery10Result(
                personId1,
                score1
        );
        LdbcSnbBiQuery10Result result2a = new LdbcSnbBiQuery10Result(
                personId2,
                score2
        );
        LdbcSnbBiQuery10Result result3a = new LdbcSnbBiQuery10Result(
                personId2,
                score1
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
        String tag1 = "さ";
        int likeCount1 = 2;
        int replyCount1 = 3;

        long personId2 = 4;
        String tag2 = "丵";
        int likeCount2 = 5;
        int replyCount2 = 6;

        LdbcSnbBiQuery11Result result1a = new LdbcSnbBiQuery11Result(
                personId1,
                tag1,
                likeCount1,
                replyCount1
        );
        LdbcSnbBiQuery11Result result1b = new LdbcSnbBiQuery11Result(
                personId1,
                tag1,
                likeCount1,
                replyCount1
        );
        LdbcSnbBiQuery11Result result2a = new LdbcSnbBiQuery11Result(
                personId2,
                tag2,
                likeCount2,
                replyCount2
        );
        LdbcSnbBiQuery11Result result3a = new LdbcSnbBiQuery11Result(
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

        LdbcSnbBiQuery12Result result1a = new LdbcSnbBiQuery12Result(
                postId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery12Result result1b = new LdbcSnbBiQuery12Result(
                postId1,
                firstName1,
                lastName1,
                createDate1,
                count1
        );
        LdbcSnbBiQuery12Result result2a = new LdbcSnbBiQuery12Result(
                postId2,
                firstName2,
                lastName2,
                createDate2,
                count2
        );
        LdbcSnbBiQuery12Result result3a = new LdbcSnbBiQuery12Result(
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
        String tag1 = "3";
        int count1 = 4;

        int year2 = 5;
        int month2 = 6;
        String tag2 = "7";
        int count2 = 8;

        LdbcSnbBiQuery13Result result1a = new LdbcSnbBiQuery13Result(
                year1,
                month1,
                tag1,
                count1
        );
        LdbcSnbBiQuery13Result result1b = new LdbcSnbBiQuery13Result(
                year1,
                month1,
                tag1,
                count1
        );
        LdbcSnbBiQuery13Result result2a = new LdbcSnbBiQuery13Result(
                year2,
                month2,
                tag2,
                count2
        );
        LdbcSnbBiQuery13Result result3a = new LdbcSnbBiQuery13Result(
                year2,
                month2,
                tag1,
                count2
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

        LdbcSnbBiQuery14Result result1a = new LdbcSnbBiQuery14Result(
                personId1,
                firstName1,
                lastName1,
                count1,
                threadCount1
        );
        LdbcSnbBiQuery14Result result1b = new LdbcSnbBiQuery14Result(
                personId1,
                firstName1,
                lastName1,
                count1,
                threadCount1
        );
        LdbcSnbBiQuery14Result result2a = new LdbcSnbBiQuery14Result(
                personId2,
                firstName2,
                lastName2,
                count2,
                threadCount2
        );
        LdbcSnbBiQuery14Result result3a = new LdbcSnbBiQuery14Result(
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

        LdbcSnbBiQuery15Result result1a = new LdbcSnbBiQuery15Result(
                personId1,
                count1
        );
        LdbcSnbBiQuery15Result result1b = new LdbcSnbBiQuery15Result(
                personId1,
                count1
        );
        LdbcSnbBiQuery15Result result2a = new LdbcSnbBiQuery15Result(
                personId2,
                count2
        );
        LdbcSnbBiQuery15Result result3a = new LdbcSnbBiQuery15Result(
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

        LdbcSnbBiQuery16Result result1a = new LdbcSnbBiQuery16Result(
                personId1,
                tag1,
                count1
        );
        LdbcSnbBiQuery16Result result1b = new LdbcSnbBiQuery16Result(
                personId1,
                tag1,
                count1
        );
        LdbcSnbBiQuery16Result result2a = new LdbcSnbBiQuery16Result(
                personId2,
                tag2,
                count2
        );
        LdbcSnbBiQuery16Result result3a = new LdbcSnbBiQuery16Result(
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

        LdbcSnbBiQuery17Result result1a = new LdbcSnbBiQuery17Result(
                count1
        );
        LdbcSnbBiQuery17Result result1b = new LdbcSnbBiQuery17Result(
                count1
        );
        LdbcSnbBiQuery17Result result2a = new LdbcSnbBiQuery17Result(
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

        LdbcSnbBiQuery18Result result1a = new LdbcSnbBiQuery18Result(
                postCount1,
                count1
        );
        LdbcSnbBiQuery18Result result1b = new LdbcSnbBiQuery18Result(
                postCount1,
                count1
        );
        LdbcSnbBiQuery18Result result2a = new LdbcSnbBiQuery18Result(
                postCount2,
                count2
        );
        LdbcSnbBiQuery18Result result3a = new LdbcSnbBiQuery18Result(
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

        LdbcSnbBiQuery19Result result1a = new LdbcSnbBiQuery19Result(
                personId1,
                strangerCount1,
                count1
        );
        LdbcSnbBiQuery19Result result1b = new LdbcSnbBiQuery19Result(
                personId1,
                strangerCount1,
                count1
        );
        LdbcSnbBiQuery19Result result2a = new LdbcSnbBiQuery19Result(
                personId2,
                strangerCount2,
                count2
        );
        LdbcSnbBiQuery19Result result3a = new LdbcSnbBiQuery19Result(
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

        LdbcSnbBiQuery20Result result1a = new LdbcSnbBiQuery20Result(
                tagClass1,
                count1
        );
        LdbcSnbBiQuery20Result result1b = new LdbcSnbBiQuery20Result(
                tagClass1,
                count1
        );
        LdbcSnbBiQuery20Result result2a = new LdbcSnbBiQuery20Result(
                tagClass2,
                count2
        );
        LdbcSnbBiQuery20Result result3a = new LdbcSnbBiQuery20Result(
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
        int score1 = 4;

        long personId2 = 5;
        int zombieCount2 = 6;
        int realCount2 = 7;
        int score2 = 8;

        LdbcSnbBiQuery21Result result1a = new LdbcSnbBiQuery21Result(
                personId1,
                zombieCount1,
                realCount1,
                score1
        );
        LdbcSnbBiQuery21Result result1b = new LdbcSnbBiQuery21Result(
                personId1,
                zombieCount1,
                realCount1,
                score1
        );
        LdbcSnbBiQuery21Result result2a = new LdbcSnbBiQuery21Result(
                personId2,
                zombieCount2,
                realCount2,
                score2
        );
        LdbcSnbBiQuery21Result result3a = new LdbcSnbBiQuery21Result(
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
        LdbcSnbBiQuery22Result result1a = new LdbcSnbBiQuery22Result(
        );
        LdbcSnbBiQuery22Result result1b = new LdbcSnbBiQuery22Result(
        );

        assertThat( result1a, equalTo( result1b ) );
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

        LdbcSnbBiQuery23Result result1a = new LdbcSnbBiQuery23Result(
                place1,
                month1,
                count1
        );
        LdbcSnbBiQuery23Result result1b = new LdbcSnbBiQuery23Result(
                place1,
                month1,
                count1
        );
        LdbcSnbBiQuery23Result result2a = new LdbcSnbBiQuery23Result(
                place2,
                month2,
                count2
        );
        LdbcSnbBiQuery23Result result3a = new LdbcSnbBiQuery23Result(
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
        int year1 = 1;
        int month1 = 2;
        String continent1 = "3";
        int postCount1 = 4;

        int year2 = 5;
        int month2 = 6;
        String continent2 = "7";
        int postCount2 = 8;

        LdbcSnbBiQuery24Result result1a = new LdbcSnbBiQuery24Result(
                year1,
                month1,
                continent1,
                postCount1
        );
        LdbcSnbBiQuery24Result result1b = new LdbcSnbBiQuery24Result(
                year1,
                month1,
                continent1,
                postCount1
        );
        LdbcSnbBiQuery24Result result2a = new LdbcSnbBiQuery24Result(
                year2,
                month2,
                continent2,
                postCount2
        );
        LdbcSnbBiQuery24Result result3a = new LdbcSnbBiQuery24Result(
                year2,
                month2,
                continent2,
                postCount1
        );

        assertThat( result1a, equalTo( result1b ) );
        assertThat( result1a, not( equalTo( result2a ) ) );
        assertThat( result1a, not( equalTo( result3a ) ) );
        assertThat( result2a, not( equalTo( result3a ) ) );
    }
}