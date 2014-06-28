package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Date;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class LdbcSnbOperationEqualityTests {
    @Test
    public void ldbcQuery1ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        String firstName1 = "3";
        int limit1 = 4;

        long personId2 = 5;
        String personUri2 = "6";
        String firstName2 = "7";
        int limit2 = 8;

        // When
        LdbcQuery1 ldbcQuery1a = new LdbcQuery1(personId1, personUri1, firstName1, limit1);
        LdbcQuery1 ldbcQuery1b = new LdbcQuery1(personId1, personUri1, firstName1, limit1);
        LdbcQuery1 ldbcQuery2a = new LdbcQuery1(personId2, personUri2, firstName2, limit2);
        LdbcQuery1 ldbcQuery3a = new LdbcQuery1(personId1, personUri1, firstName1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery2ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        Date maxDate1 = new Date(3);
        int limit1 = 4;

        long personId2 = 5;
        String personUri2 = "6";
        Date maxDate2 = new Date(7);
        int limit2 = 8;

        // When
        LdbcQuery2 ldbcQuery1a = new LdbcQuery2(personId1, personUri1, maxDate1, limit1);
        LdbcQuery2 ldbcQuery1b = new LdbcQuery2(personId1, personUri1, maxDate1, limit1);
        LdbcQuery2 ldbcQuery2a = new LdbcQuery2(personId2, personUri2, maxDate2, limit2);
        LdbcQuery2 ldbcQuery3a = new LdbcQuery2(personId1, personUri1, maxDate1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery3ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        String countryXName1 = "3";
        String countryYName1 = "4";
        Date startDate1 = new Date(5);
        int durationDays1 = 6;
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        String countryXName2 = "10";
        String countryYName2 = "11";
        Date startDate2 = new Date(12);
        int durationDays2 = 13;
        int limit2 = 14;

        // When
        LdbcQuery3 ldbcQuery1a = new LdbcQuery3(personId1, personUri1, countryXName1, countryYName1, startDate1, durationDays1, limit1);
        LdbcQuery3 ldbcQuery1b = new LdbcQuery3(personId1, personUri1, countryXName1, countryYName1, startDate1, durationDays1, limit1);
        LdbcQuery3 ldbcQuery2a = new LdbcQuery3(personId2, personUri2, countryXName2, countryYName2, startDate2, durationDays2, limit2);
        LdbcQuery3 ldbcQuery3a = new LdbcQuery3(personId1, personUri1, countryXName1, countryYName1, startDate1, durationDays1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery4ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        Date startDate1 = new Date(5);
        int durationDays1 = 6;
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        Date startDate2 = new Date(12);
        int durationDays2 = 13;
        int limit2 = 14;

        // When
        LdbcQuery4 ldbcQuery1a = new LdbcQuery4(personId1, personUri1, startDate1, durationDays1, limit1);
        LdbcQuery4 ldbcQuery1b = new LdbcQuery4(personId1, personUri1, startDate1, durationDays1, limit1);
        LdbcQuery4 ldbcQuery2a = new LdbcQuery4(personId2, personUri2, startDate2, durationDays2, limit2);
        LdbcQuery4 ldbcQuery3a = new LdbcQuery4(personId1, personUri1, startDate1, durationDays1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery5ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        Date minDate1 = new Date(5);
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        Date minDate2 = new Date(12);
        int limit2 = 14;

        // When
        LdbcQuery5 ldbcQuery1a = new LdbcQuery5(personId1, personUri1, minDate1, limit1);
        LdbcQuery5 ldbcQuery1b = new LdbcQuery5(personId1, personUri1, minDate1, limit1);
        LdbcQuery5 ldbcQuery2a = new LdbcQuery5(personId2, personUri2, minDate2, limit2);
        LdbcQuery5 ldbcQuery3a = new LdbcQuery5(personId1, personUri1, minDate1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery6ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        String tagName1 = "3";
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        String tagName2 = "10";
        int limit2 = 14;

        // When
        LdbcQuery6 ldbcQuery1a = new LdbcQuery6(personId1, personUri1, tagName1, limit1);
        LdbcQuery6 ldbcQuery1b = new LdbcQuery6(personId1, personUri1, tagName1, limit1);
        LdbcQuery6 ldbcQuery2a = new LdbcQuery6(personId2, personUri2, tagName2, limit2);
        LdbcQuery6 ldbcQuery3a = new LdbcQuery6(personId1, personUri1, tagName1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery7ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        int limit2 = 14;

        // When
        LdbcQuery7 ldbcQuery1a = new LdbcQuery7(personId1, personUri1, limit1);
        LdbcQuery7 ldbcQuery1b = new LdbcQuery7(personId1, personUri1, limit1);
        LdbcQuery7 ldbcQuery2a = new LdbcQuery7(personId2, personUri2, limit2);
        LdbcQuery7 ldbcQuery3a = new LdbcQuery7(personId1, personUri1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery8ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        int limit2 = 14;

        // When
        LdbcQuery8 ldbcQuery1a = new LdbcQuery8(personId1, personUri1, limit1);
        LdbcQuery8 ldbcQuery1b = new LdbcQuery8(personId1, personUri1, limit1);
        LdbcQuery8 ldbcQuery2a = new LdbcQuery8(personId2, personUri2, limit2);
        LdbcQuery8 ldbcQuery3a = new LdbcQuery8(personId1, personUri1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery9ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        Date maxDate1 = new Date(3);
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        Date maxDate2 = new Date(10);
        int limit2 = 14;

        // When
        LdbcQuery9 ldbcQuery1a = new LdbcQuery9(personId1, personUri1, maxDate1, limit1);
        LdbcQuery9 ldbcQuery1b = new LdbcQuery9(personId1, personUri1, maxDate1, limit1);
        LdbcQuery9 ldbcQuery2a = new LdbcQuery9(personId2, personUri2, maxDate2, limit2);
        LdbcQuery9 ldbcQuery3a = new LdbcQuery9(personId1, personUri1, maxDate1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery10ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        int month11 = 3;
        int month21 = 4;
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        int month12 = 10;
        int month22 = 11;
        int limit2 = 12;

        // When
        LdbcQuery10 ldbcQuery1a = new LdbcQuery10(personId1, personUri1, month11, month21, limit1);
        LdbcQuery10 ldbcQuery1b = new LdbcQuery10(personId1, personUri1, month11, month21, limit1);
        LdbcQuery10 ldbcQuery2a = new LdbcQuery10(personId2, personUri2, month12, month22, limit2);
        LdbcQuery10 ldbcQuery3a = new LdbcQuery10(personId1, personUri1, month11, month21, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery11ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        String countryName1 = "3";
        int workFromYear1 = 4;
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        String countryName2 = "10";
        int workFromYear2 = 11;
        int limit2 = 12;

        // When
        LdbcQuery11 ldbcQuery1a = new LdbcQuery11(personId1, personUri1, countryName1, workFromYear1, limit1);
        LdbcQuery11 ldbcQuery1b = new LdbcQuery11(personId1, personUri1, countryName1, workFromYear1, limit1);
        LdbcQuery11 ldbcQuery2a = new LdbcQuery11(personId2, personUri2, countryName2, workFromYear2, limit2);
        LdbcQuery11 ldbcQuery3a = new LdbcQuery11(personId1, personUri1, countryName1, workFromYear1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery12ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personUri1 = "2";
        String tagClassName1 = "3";
        int limit1 = 7;

        long personId2 = 8;
        String personUri2 = "9";
        String tagClassName2 = "10";
        int limit2 = 12;

        // When
        LdbcQuery12 ldbcQuery1a = new LdbcQuery12(personId1, personUri1, tagClassName1, limit1);
        LdbcQuery12 ldbcQuery1b = new LdbcQuery12(personId1, personUri1, tagClassName1, limit1);
        LdbcQuery12 ldbcQuery2a = new LdbcQuery12(personId2, personUri2, tagClassName2, limit2);
        LdbcQuery12 ldbcQuery3a = new LdbcQuery12(personId1, personUri1, tagClassName1, limit2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery13ShouldDoEqualsCorrectly() {
        // Given
        long person1Id1 = 1;
        String person1Uri1 = "2";
        long person2Id1 = 3;
        String person2Uri1 = "4";

        long person1Id2 = 5;
        String person1Uri2 = "6";
        long person2Id2 = 7;
        String person2Uri2 = "8";

        // When
        LdbcQuery13 ldbcQuery1a = new LdbcQuery13(person1Id1, person1Uri1, person2Id1, person2Uri1);
        LdbcQuery13 ldbcQuery1b = new LdbcQuery13(person1Id1, person1Uri1, person2Id1, person2Uri1);
        LdbcQuery13 ldbcQuery2a = new LdbcQuery13(person1Id2, person1Uri2, person2Id2, person2Uri2);
        LdbcQuery13 ldbcQuery3a = new LdbcQuery13(person1Id1, person1Uri1, person2Id1, person2Uri2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcQuery14ShouldDoEqualsCorrectly() {
        // Given
        long person1Id1 = 1;
        String person1Uri1 = "2";
        long person2Id1 = 3;
        String person2Uri1 = "4";

        long person1Id2 = 5;
        String person1Uri2 = "6";
        long person2Id2 = 7;
        String person2Uri2 = "8";

        // When
        LdbcQuery13 ldbcQuery1a = new LdbcQuery13(person1Id1, person1Uri1, person2Id1, person2Uri1);
        LdbcQuery13 ldbcQuery1b = new LdbcQuery13(person1Id1, person1Uri1, person2Id1, person2Uri1);
        LdbcQuery13 ldbcQuery2a = new LdbcQuery13(person1Id2, person1Uri2, person2Id2, person2Uri2);
        LdbcQuery13 ldbcQuery3a = new LdbcQuery13(person1Id1, person1Uri1, person2Id1, person2Uri2);

        // Then
        assertThat(ldbcQuery1a, equalTo(ldbcQuery1b));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery2a)));
        assertThat(ldbcQuery1a, not(equalTo(ldbcQuery3a)));
        assertThat(ldbcQuery2a, not(equalTo(ldbcQuery3a)));
    }

    @Test
    public void ldbcUpdate1ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        String personFirstName1 = "2";
        String personLastName1 = "3";
        String gender1 = "4";
        Date birthday1 = new Date(5);
        Date creationDate1 = new Date(6);
        String locationIp1 = "7";
        String browserUsed1 = "8";
        long cityId1 = 9;
        List<String> languages1 = Lists.newArrayList("10");
        List<String> emails1 = Lists.newArrayList("11", "12");
        List<Long> tagIds1 = Lists.newArrayList();
        List<LdbcUpdate1AddPerson.Organization> studyAt1 = Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(13, 14));
        List<LdbcUpdate1AddPerson.Organization> workAt1 = Lists.newArrayList(
                new LdbcUpdate1AddPerson.Organization(15, 16),
                new LdbcUpdate1AddPerson.Organization(17, 18));

        long personId2 = 19;
        String personFirstName2 = "20";
        String personLastName2 = "21";
        String gender2 = "22";
        Date birthday2 = new Date(23);
        Date creationDate2 = new Date(24);
        String locationIp2 = "25";
        String browserUsed2 = "26";
        long cityId2 = 27;
        List<String> languages2 = Lists.newArrayList();
        List<String> emails2 = Lists.newArrayList("28", "29");
        List<Long> tagIds2 = Lists.newArrayList(30l);
        List<LdbcUpdate1AddPerson.Organization> studyAt2 = Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(31, 32));
        List<LdbcUpdate1AddPerson.Organization> workAt2 = Lists.newArrayList();

        // When
        LdbcUpdate1AddPerson ldbcUpdate1a = new LdbcUpdate1AddPerson(
                personId1,
                personFirstName1,
                personLastName1,
                gender1,
                birthday1,
                creationDate1,
                locationIp1,
                browserUsed1,
                cityId1, languages1, emails1, tagIds1, studyAt1, workAt1);
        LdbcUpdate1AddPerson ldbcUpdate1b = new LdbcUpdate1AddPerson(
                personId1,
                personFirstName1,
                personLastName1,
                gender1,
                birthday1,
                creationDate1,
                locationIp1,
                browserUsed1,
                cityId1, languages1, emails1, tagIds1, studyAt1, workAt1);
        LdbcUpdate1AddPerson ldbcUpdate2a = new LdbcUpdate1AddPerson(
                personId2,
                personFirstName2,
                personLastName2,
                gender2,
                birthday2,
                creationDate2,
                locationIp2,
                browserUsed2,
                cityId2, languages2, emails2, tagIds2, studyAt2, workAt2);
        LdbcUpdate1AddPerson ldbcUpdate2b = new LdbcUpdate1AddPerson(
                personId2,
                personFirstName2,
                personLastName2,
                gender2,
                birthday2,
                creationDate2,
                locationIp2,
                browserUsed2,
                cityId2, languages2, emails2, tagIds2, studyAt2, workAt2);
        LdbcUpdate1AddPerson ldbcUpdate3a = new LdbcUpdate1AddPerson(
                personId1,
                personFirstName1,
                personLastName1,
                gender1,
                birthday1,
                creationDate1,
                locationIp1,
                browserUsed1,
                cityId1, languages1, emails1, tagIds1, studyAt1, workAt2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate2a, equalTo(ldbcUpdate2b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate2ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        long postId1 = 2;
        Date creationDate1 = new Date(3);

        long personId2 = 4;
        long postId2 = 5;
        Date creationDate2 = new Date(6);

        // When
        LdbcUpdate2AddPostLike ldbcUpdate1a = new LdbcUpdate2AddPostLike(personId1, postId1, creationDate1);
        LdbcUpdate2AddPostLike ldbcUpdate1b = new LdbcUpdate2AddPostLike(personId1, postId1, creationDate1);
        LdbcUpdate2AddPostLike ldbcUpdate2a = new LdbcUpdate2AddPostLike(personId2, postId2, creationDate2);
        LdbcUpdate2AddPostLike ldbcUpdate3a = new LdbcUpdate2AddPostLike(personId1, postId1, creationDate2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate3ShouldDoEqualsCorrectly() {
        // Given
        long personId1 = 1;
        long commentId1 = 2;
        Date creationDate1 = new Date(3);

        long personId2 = 4;
        long commentId2 = 5;
        Date creationDate2 = new Date(6);

        // When
        LdbcUpdate3AddCommentLike ldbcUpdate1a = new LdbcUpdate3AddCommentLike(personId1, commentId1, creationDate1);
        LdbcUpdate3AddCommentLike ldbcUpdate1b = new LdbcUpdate3AddCommentLike(personId1, commentId1, creationDate1);
        LdbcUpdate3AddCommentLike ldbcUpdate2a = new LdbcUpdate3AddCommentLike(personId2, commentId2, creationDate2);
        LdbcUpdate3AddCommentLike ldbcUpdate3a = new LdbcUpdate3AddCommentLike(personId1, commentId1, creationDate2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate4ShouldDoEqualsCorrectly() {
        // Given
        long forumId1 = 1;
        String forumTitle1 = "2";
        Date creationDate1 = new Date(3);
        long moderatorPersonId1 = 4;
        List<Long> tagIds1 = Lists.newArrayList(5l, 6l);

        long forumId2 = 7;
        String forumTitle2 = "8";
        Date creationDate2 = new Date(9);
        long moderatorPersonId2 = 10;
        List<Long> tagIds2 = Lists.newArrayList();

        // When
        LdbcUpdate4AddForum ldbcUpdate1a = new LdbcUpdate4AddForum(forumId1, forumTitle1, creationDate1, moderatorPersonId1, tagIds1);
        LdbcUpdate4AddForum ldbcUpdate1b = new LdbcUpdate4AddForum(forumId1, forumTitle1, creationDate1, moderatorPersonId1, tagIds1);
        LdbcUpdate4AddForum ldbcUpdate2a = new LdbcUpdate4AddForum(forumId2, forumTitle2, creationDate2, moderatorPersonId2, tagIds2);
        LdbcUpdate4AddForum ldbcUpdate3a = new LdbcUpdate4AddForum(forumId1, forumTitle1, creationDate1, moderatorPersonId1, tagIds2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate5ShouldDoEqualsCorrectly() {
        // Given
        long forumId1 = 1;
        long personId1 = 2;
        Date creationDate1 = new Date(3);

        long forumId2 = 4;
        long personId2 = 5;
        Date creationDate2 = new Date(6);

        // When
        LdbcUpdate5AddForumMembership ldbcUpdate1a = new LdbcUpdate5AddForumMembership(forumId1, personId1, creationDate1);
        LdbcUpdate5AddForumMembership ldbcUpdate1b = new LdbcUpdate5AddForumMembership(forumId1, personId1, creationDate1);
        LdbcUpdate5AddForumMembership ldbcUpdate2a = new LdbcUpdate5AddForumMembership(forumId2, personId2, creationDate2);
        LdbcUpdate5AddForumMembership ldbcUpdate3a = new LdbcUpdate5AddForumMembership(forumId1, personId1, creationDate2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate6ShouldDoEqualsCorrectly() {
        // Given
        long postId1 = 1;
        String imageFile1 = "2";
        Date creationDate1 = new Date(3);
        String locationIp1 = "4";
        String browserUsed1 = "5";
        String language1 = "6";
        String content1 = "7";
        int length1 = 8;
        long authorPersonId1 = 9;
        long forumId1 = 10;
        long countryId1 = 11;
        List<Long> tagIds1 = Lists.newArrayList(12l);

        long postId2 = 13;
        String imageFile2 = "14";
        Date creationDate2 = new Date(15);
        String locationIp2 = "16";
        String browserUsed2 = "17";
        String language2 = "18";
        String content2 = "19";
        int length2 = 20;
        long authorPersonId2 = 21;
        long forumId2 = 22;
        long countryId2 = 23;
        List<Long> tagIds2 = Lists.newArrayList(24l, 25l, 26l);

        // When
        LdbcUpdate6AddPost ldbcUpdate1a = new LdbcUpdate6AddPost(
                postId1,
                imageFile1,
                creationDate1,
                locationIp1,
                browserUsed1,
                language1,
                content1,
                length1,
                authorPersonId1,
                forumId1,
                countryId1,
                tagIds1);

        LdbcUpdate6AddPost ldbcUpdate1b = new LdbcUpdate6AddPost(
                postId1,
                imageFile1,
                creationDate1,
                locationIp1,
                browserUsed1,
                language1,
                content1,
                length1,
                authorPersonId1,
                forumId1,
                countryId1,
                tagIds1);

        LdbcUpdate6AddPost ldbcUpdate2a = new LdbcUpdate6AddPost(
                postId2,
                imageFile2,
                creationDate2,
                locationIp2,
                browserUsed2,
                language2,
                content2,
                length2,
                authorPersonId2,
                forumId2,
                countryId2,
                tagIds2);

        LdbcUpdate6AddPost ldbcUpdate3a = new LdbcUpdate6AddPost(
                postId1,
                imageFile1,
                creationDate1,
                locationIp1,
                browserUsed1,
                language1,
                content1,
                length1,
                authorPersonId1,
                forumId1,
                countryId1,
                tagIds2);


        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate7ShouldDoEqualsCorrectly() {
        // Given
        long commentId1 = 1;
        Date creationDate1 = new Date(2);
        String locationIp1 = "3";
        String browserUsed1 = "4";
        String content1 = "5";
        int length1 = 6;
        long authorPersonId1 = 7;
        long countryId1 = 8;
        long replyToPostId1 = 9;
        long replyToCommentId1 = 10;
        List<Long> tagIds1 = Lists.newArrayList();

        long commentId2 = 11;
        Date creationDate2 = new Date(12);
        String locationIp2 = "13";
        String browserUsed2 = "14";
        String content2 = "15";
        int length2 = 16;
        long authorPersonId2 = 17;
        long countryId2 = 18;
        long replyToPostId2 = 19;
        long replyToCommentId2 = 20;
        List<Long> tagIds2 = Lists.newArrayList(21l);

        // When
        LdbcUpdate7AddComment ldbcUpdate1a = new LdbcUpdate7AddComment(
                commentId1,
                creationDate1,
                locationIp1,
                browserUsed1,
                content1,
                length1,
                authorPersonId1,
                countryId1,
                replyToPostId1,
                replyToCommentId1,
                tagIds1);

        LdbcUpdate7AddComment ldbcUpdate1b = new LdbcUpdate7AddComment(
                commentId1,
                creationDate1,
                locationIp1,
                browserUsed1,
                content1,
                length1,
                authorPersonId1,
                countryId1,
                replyToPostId1,
                replyToCommentId1,
                tagIds1);

        LdbcUpdate7AddComment ldbcUpdate2a = new LdbcUpdate7AddComment(
                commentId2,
                creationDate2,
                locationIp2,
                browserUsed2,
                content2,
                length2,
                authorPersonId2,
                countryId2,
                replyToPostId2,
                replyToCommentId2,
                tagIds2);

        LdbcUpdate7AddComment ldbcUpdate3a = new LdbcUpdate7AddComment(
                commentId1,
                creationDate1,
                locationIp1,
                browserUsed1,
                content1,
                length1,
                authorPersonId1,
                countryId1,
                replyToPostId1,
                replyToCommentId1,
                tagIds2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }

    @Test
    public void ldbcUpdate8ShouldDoEqualsCorrectly() {
        // Given
        long person1Id1 = 1;
        long person2Id1 = 2;
        Date creationDate1 = new Date(3);

        long person1Id2 = 4;
        long person2Id2 = 5;
        Date creationDate2 = new Date(6);

        // When
        LdbcUpdate8AddFriendship ldbcUpdate1a = new LdbcUpdate8AddFriendship(person1Id1, person2Id1, creationDate1);
        LdbcUpdate8AddFriendship ldbcUpdate1b = new LdbcUpdate8AddFriendship(person1Id1, person2Id1, creationDate1);
        LdbcUpdate8AddFriendship ldbcUpdate2a = new LdbcUpdate8AddFriendship(person1Id2, person2Id2, creationDate2);
        LdbcUpdate8AddFriendship ldbcUpdate3a = new LdbcUpdate8AddFriendship(person1Id1, person2Id1, creationDate2);

        // Then
        assertThat(ldbcUpdate1a, equalTo(ldbcUpdate1b));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate2a)));
        assertThat(ldbcUpdate1a, not(equalTo(ldbcUpdate3a)));
        assertThat(ldbcUpdate2a, not(equalTo(ldbcUpdate3a)));
    }
}