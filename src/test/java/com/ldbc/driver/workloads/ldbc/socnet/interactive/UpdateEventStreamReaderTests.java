package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import org.junit.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class UpdateEventStreamReaderTests {
    @Test
    public void shouldParseUpdate1AddPerson() throws IOException, ParseException {
        // Given
        UpdateEventStreamReader updateEventStreamReader = new UpdateEventStreamReader(null);
        String jsonString = "[" +
                "409," +
                "\"Lei\"," +
                "\"Zhao\"," +
                "\"male\"," +
                "\"1989-07-21\"," +
                "\"2011-01-18\"," +
                "\"14.131.98.220\"," +
                "\"Chrome\"," +
                "392," +
                "[\"english\",\"swedish\"]," +
                "[\"user@email.com\"]," +
                "[1612]," +
                "[[97,1]]," +
                "[[911,1970],[935,1970],[913,1971],[1539,1971]]" +
                "]";

        // When
        LdbcUpdate1AddPerson addPerson = updateEventStreamReader.parseAddPerson(jsonString);

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(1989, Calendar.JULY, 21);
        Date birthday = c.getTime();
        c.clear();
        c.set(2011, Calendar.JANUARY, 18);
        Date creationDate = c.getTime();

        assertThat(addPerson.personId(), is(409L));
        assertThat(addPerson.personFirstName(), equalTo("Lei"));
        assertThat(addPerson.personLastName(), equalTo("Zhao"));
        assertThat(addPerson.gender(), equalTo("male"));
        assertThat(addPerson.birthday(), equalTo(birthday));
        assertThat(addPerson.creationDate(), equalTo(creationDate));
        assertThat(addPerson.locationIp(), equalTo("14.131.98.220"));
        assertThat(addPerson.browserUsed(), equalTo("Chrome"));
        assertThat(addPerson.cityId(), is(392L));
        assertThat(addPerson.languages(), equalTo(new String[]{"english", "swedish"}));
        assertThat(addPerson.emails(), equalTo(new String[]{"user@email.com"}));
        assertThat(addPerson.tagIds(), equalTo(new long[]{1612L}));
        assertThat(addPerson.studyAt(), equalTo(new LdbcUpdate1AddPerson.Organization[]{
                new LdbcUpdate1AddPerson.Organization(97L, 1)}));
        assertThat(addPerson.workAt(), equalTo(new LdbcUpdate1AddPerson.Organization[]{
                new LdbcUpdate1AddPerson.Organization(911L, 1970),
                new LdbcUpdate1AddPerson.Organization(935L, 1970),
                new LdbcUpdate1AddPerson.Organization(913L, 1971),
                new LdbcUpdate1AddPerson.Organization(1539L, 1971)}));
    }

    @Test
    public void shouldParseUpdate2AddLikePost() throws IOException, ParseException {
        // Given
        UpdateEventStreamReader updateEventStreamReader = new UpdateEventStreamReader(null);
        String jsonString = "[1582,120207,\"2011-02-01T08:36:04Z\"]";

        // When
        LdbcUpdate2AddPostLike addPostLike = updateEventStreamReader.parseAddPostLike(jsonString);

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.FEBRUARY, 01, 8, 36, 04);
        Date creationDate = c.getTime();

        assertThat(addPostLike.personId(), is(1582L));
        assertThat(addPostLike.postId(), is(120207L));
        assertThat(addPostLike.creationDate(), equalTo(creationDate));
    }

    @Test
    public void shouldParseUpdate3AddLikeComment() throws IOException, ParseException {
        // Given
        UpdateEventStreamReader updateEventStreamReader = new UpdateEventStreamReader(null);
        String jsonString = "[1095,120426,\"2011-01-24T05:44:13Z\"]";

        // When
        LdbcUpdate3AddCommentLike addCommentLike = updateEventStreamReader.parseAddCommentLike(jsonString);

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 24, 5, 44, 13);
        Date creationDate = c.getTime();

        assertThat(addCommentLike.personId(), is(1095L));
        assertThat(addCommentLike.commentId(), is(120426L));
        assertThat(addCommentLike.creationDate(), equalTo(creationDate));
    }

    @Test
    public void shouldParseUpdate4AddForum() throws IOException, ParseException {
        // Given
        UpdateEventStreamReader updateEventStreamReader = new UpdateEventStreamReader(null);
        String jsonString = "[2118,\"Group for The_Beekeeper in Pakistan\",\"2011-01-03T06:04:47Z\",989,[10716]]";

        // When
        LdbcUpdate4AddForum addForum = updateEventStreamReader.parseAddForum(jsonString);

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 3, 6, 4, 47);
        Date creationDate = c.getTime();

        assertThat(addForum.forumId(), is(2118L));
        assertThat(addForum.forumTitle(), equalTo("Group for The_Beekeeper in Pakistan"));
        assertThat(addForum.creationDate(), equalTo(creationDate));
        assertThat(addForum.moderatorPersonId(), is(989L));
        assertThat(addForum.tagIds(), equalTo(new long[]{10716}));
    }

    @Test
    public void shouldParseUpdate5AddForumMembership() throws IOException, ParseException {
        // Given
        UpdateEventStreamReader updateEventStreamReader = new UpdateEventStreamReader(null);
        String jsonString = "[2153,372,\"2011-01-04T18:42:51Z\"]";

        // When
        LdbcUpdate5AddForumMembership addForumMembership = updateEventStreamReader.parseAddForumMembership(jsonString);

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 4, 18, 42, 51);
        Date creationDate = c.getTime();

        assertThat(addForumMembership.forumId(), is(2153L));
        assertThat(addForumMembership.personId(), is(372L));
        assertThat(addForumMembership.creationDate(), equalTo(creationDate));
    }
            /*
1	:	PostId
2	:	ImageFile
3	:	CreationDate
4	:	Ip
5	:	Browser
6	:	Language
7	:	Content
8	:	Length
9	:	AuthorId
10	:	ForumId
11	:	Location
12	:	Tags
         */
            @Test
            public void shouldParseUpdate6AddPost() throws IOException, ParseException {
                // Given
//                UpdateEventStreamReader updateEventStreamReader = new UpdateEventStreamReader(null);
//                String jsonString = "[" +
//                        "120343," +
//                        "\"\"," +
//                        "\"2011-01-30T07:59:58Z\"," +
//                        "\"91.229.229.89\"," +
//                        "\"Internet Explorer\"," +
//                        "\"\"," +
//                        "\"About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.\"," +
//                        "172," +
//                        "1673," +
//                        "2152," +
//                        "9," +
//                        "[1437]]";
//
//                // When
//                LdbcUpdate5AddForumMembership addForumMembership = updateEventStreamReader.parseAddForumMembership(jsonString);
//
//                // Then
//                Calendar c = Calendar.getInstance();
//                c.clear();
//                c.set(2011, Calendar.JANUARY, 4, 18, 42, 51);
//                Date creationDate = c.getTime();
//
//                assertThat(addForumMembership.forumId(), is(2153L));
//                assertThat(addForumMembership.personId(), is(372L));
//                assertThat(addForumMembership.creationDate(), equalTo(creationDate));
            }

}
