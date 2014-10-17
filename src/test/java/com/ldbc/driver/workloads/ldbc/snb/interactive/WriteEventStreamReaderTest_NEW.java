package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.generator.RandomDataGeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.SystemTimeSource;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.temporal.TimeSource;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.util.Bucket.DiscreteBucket;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.csv.BufferedCharSeeker;
import com.ldbc.driver.util.csv.CharSeeker;
import com.ldbc.driver.util.csv.SimpleCsvFileReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.*;
import java.text.ParseException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WriteEventStreamReaderTest_NEW {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();


    TimeSource timeSource = new SystemTimeSource();

    @Test
    public void shouldParseAllEventTypesWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.ROWS_FOR_ALL_EVENT_TYPES;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseAllEventTypes(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseAllEventTypesWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.ROWS_FOR_ALL_EVENT_TYPES;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseAllEventTypes(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseAllEventTypes(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        // Then
        Calendar calendar;
        Date birthday;
        Date creationDate;

        LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        calendar.clear();
        calendar.set(1989, Calendar.JULY, 21);
        birthday = calendar.getTime();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 18, 8, 36, 4);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addPerson.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPerson.personId(), is(409L));
        assertThat(addPerson.personFirstName(), equalTo("Lei"));
        assertThat(addPerson.personLastName(), equalTo("Zhao"));
        assertThat(addPerson.gender(), equalTo("male"));
        assertThat(addPerson.birthday(), equalTo(birthday));
        assertThat(addPerson.creationDate(), equalTo(creationDate));
        assertThat(addPerson.locationIp(), equalTo("14.131.98.220"));
        assertThat(addPerson.browserUsed(), equalTo("Chrome"));
        assertThat(addPerson.cityId(), is(392L));
        assertThat(addPerson.languages(), equalTo((List) Lists.newArrayList("english", "swedish")));
        assertThat(addPerson.emails(), equalTo((List) Lists.newArrayList("user@email.com")));
        assertThat(addPerson.tagIds(), equalTo((List) Lists.newArrayList(1612L)));
        assertThat(addPerson.studyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
        assertThat(addPerson.workAt(), equalTo((List) Lists.newArrayList(
                new LdbcUpdate1AddPerson.Organization(911L, 1970),
                new LdbcUpdate1AddPerson.Organization(935L, 1970),
                new LdbcUpdate1AddPerson.Organization(913L, 1971),
                new LdbcUpdate1AddPerson.Organization(1539L, 1971)
        )));

        LdbcUpdate2AddPostLike addPostLike = (LdbcUpdate2AddPostLike) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2011, Calendar.FEBRUARY, 01, 8, 36, 04);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addPostLike.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPostLike.personId(), is(1582L));
        assertThat(addPostLike.postId(), is(120207L));
        assertThat(addPostLike.creationDate(), equalTo(creationDate));

        LdbcUpdate3AddCommentLike addCommentLike = (LdbcUpdate3AddCommentLike) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 24, 5, 44, 13);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addCommentLike.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addCommentLike.personId(), is(1095L));
        assertThat(addCommentLike.commentId(), is(120426L));
        assertThat(addCommentLike.creationDate(), equalTo(creationDate));

        LdbcUpdate4AddForum addForum = (LdbcUpdate4AddForum) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 3, 6, 4, 47);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addForum.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addForum.forumId(), is(2118L));
        assertThat(addForum.forumTitle(), equalTo("Group for The_Beekeeper in Pakistan"));
        assertThat(addForum.creationDate(), equalTo(creationDate));
        assertThat(addForum.moderatorPersonId(), is(989L));
        assertThat(addForum.tagIds(), equalTo((List) Lists.newArrayList(10716l)));

        LdbcUpdate5AddForumMembership addForumMembership = (LdbcUpdate5AddForumMembership) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 4, 18, 42, 51);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addForumMembership.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addForumMembership.forumId(), is(2153L));
        assertThat(addForumMembership.personId(), is(372L));
        assertThat(addForumMembership.creationDate(), equalTo(creationDate));

        LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 30, 7, 59, 58);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addPost.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPost.postId(), is(120343L));
        assertThat(addPost.imageFile(), equalTo(""));
        assertThat(addPost.creationDate(), equalTo(creationDate));
        assertThat(addPost.locationIp(), equalTo("91.229.229.89"));
        assertThat(addPost.browserUsed(), equalTo("Internet Explorer"));
        assertThat(addPost.language(), equalTo(""));
        assertThat(addPost.content(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
        assertThat(addPost.length(), is(172));
        assertThat(addPost.authorPersonId(), is(1673L));
        assertThat(addPost.forumId(), is(2152L));
        assertThat(addPost.countryId(), is(9L));
        assertThat(addPost.tagIds(), equalTo((List) Lists.newArrayList(1437l)));

        LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 31, 23, 58, 49);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addComment.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addComment.commentId(), is(4034293L));
        assertThat(addComment.creationDate(), equalTo(creationDate));
        assertThat(addComment.locationIp(), equalTo("200.11.32.131"));
        assertThat(addComment.browserUsed(), equalTo("Firefox"));
        assertThat(addComment.content(), equalTo("words"));
        assertThat(addComment.length(), is(169));
        assertThat(addComment.authorPersonId(), is(7460L));
        assertThat(addComment.countryId(), is(91L));
        assertThat(addComment.replyToPostId(), is(-1L));
        assertThat(addComment.replyToCommentId(), is(4034289L));
        assertThat(addComment.tagIds(), equalTo((List) Lists.newArrayList(1403l, 1990l, 2009l, 2081l, 2817l, 2855l, 2987l, 6316l, 7425l, 8224l, 8466l)));

        addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2013, Calendar.JANUARY, 31, 23, 58, 49);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addComment.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addComment.commentId(), is(4034293L));
        assertThat(addComment.creationDate(), equalTo(creationDate));
        assertThat(addComment.locationIp(), equalTo("200.11.32.131"));
        assertThat(addComment.browserUsed(), equalTo("Firefox"));
        assertThat(addComment.content(), equalTo("words"));
        assertThat(addComment.length(), is(169));
        assertThat(addComment.authorPersonId(), is(7460L));
        assertThat(addComment.countryId(), is(91L));
        assertThat(addComment.replyToPostId(), is(-1L));
        assertThat(addComment.replyToCommentId(), is(4034289L));
        assertThat(addComment.tagIds(), equalTo((List) Lists.newArrayList()));

        LdbcUpdate8AddFriendship addFriendship = (LdbcUpdate8AddFriendship) writeEventStreamReader.next();
        calendar = Calendar.getInstance();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 10, 15, 58, 45);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        creationDate = calendar.getTime();

        assertThat(addFriendship.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addFriendship.person1Id(), is(1920L));
        assertThat(addFriendship.person2Id(), is(655L));
        assertThat(addFriendship.creationDate(), equalTo(creationDate));

        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate1AddPersonWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_1_ADD_PERSON_ROW;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate1AddPerson(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_1_ADD_PERSON_ROW;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate1AddPerson(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPerson(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

        // Then
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        calendar.clear();
        calendar.set(1989, Calendar.JULY, 21);
        Date birthday = calendar.getTime();
        calendar.clear();
        calendar.set(2011, Calendar.JANUARY, 18, 8, 36, 4);
        calendar.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = calendar.getTime();

        assertThat(addPerson.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPerson.personId(), is(409L));
        assertThat(addPerson.personFirstName(), equalTo("Lei"));
        assertThat(addPerson.personLastName(), equalTo("Zhao"));
        assertThat(addPerson.gender(), equalTo("male"));
        assertThat(addPerson.birthday(), equalTo(birthday));
        assertThat(addPerson.creationDate(), equalTo(creationDate));
        assertThat(addPerson.locationIp(), equalTo("14.131.98.220"));
        assertThat(addPerson.browserUsed(), equalTo("Chrome"));
        assertThat(addPerson.cityId(), is(392L));
        assertThat(addPerson.languages(), equalTo((List) Lists.newArrayList("english", "swedish")));
        assertThat(addPerson.emails(), equalTo((List) Lists.newArrayList("user@email.com")));
        assertThat(addPerson.tagIds(), equalTo((List) Lists.newArrayList(1612L)));
        assertThat(addPerson.studyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
        assertThat(addPerson.workAt(), equalTo((List) Lists.newArrayList(
                new LdbcUpdate1AddPerson.Organization(911L, 1970),
                new LdbcUpdate1AddPerson.Organization(935L, 1970),
                new LdbcUpdate1AddPerson.Organization(913L, 1971),
                new LdbcUpdate1AddPerson.Organization(1539L, 1971)
        )));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate2AddLikePostWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_2_ADD_LIKE_POST_ROW;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate2AddLikePost(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate2AddLikePostWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_2_ADD_LIKE_POST_ROW;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate2AddLikePost(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate2AddLikePost(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate2AddPostLike addPostLike = (LdbcUpdate2AddPostLike) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.FEBRUARY, 01, 8, 36, 04);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addPostLike.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPostLike.personId(), is(1582L));
        assertThat(addPostLike.postId(), is(120207L));
        assertThat(addPostLike.creationDate(), equalTo(creationDate));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate3AddLikeCommentWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_3_ADD_LIKE_COMMENT;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate3AddLikeComment(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate3AddLikeCommentWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_3_ADD_LIKE_COMMENT;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate3AddLikeComment(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate3AddLikeComment(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate3AddCommentLike addCommentLike = (LdbcUpdate3AddCommentLike) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 24, 5, 44, 13);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addCommentLike.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addCommentLike.personId(), is(1095L));
        assertThat(addCommentLike.commentId(), is(120426L));
        assertThat(addCommentLike.creationDate(), equalTo(creationDate));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate4AddForumWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_4_ADD_FORUM;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate4AddForum(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate4AddForumWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_4_ADD_FORUM;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate4AddForum(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate4AddForum(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate4AddForum addForum = (LdbcUpdate4AddForum) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 3, 6, 4, 47);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addForum.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addForum.forumId(), is(2118L));
        assertThat(addForum.forumTitle(), equalTo("Group for The_Beekeeper in Pakistan"));
        assertThat(addForum.creationDate(), equalTo(creationDate));
        assertThat(addForum.moderatorPersonId(), is(989L));
        assertThat(addForum.tagIds(), equalTo((List) Lists.newArrayList(10716l)));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate5AddForumMembershipWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_5_ADD_FORUM_MEMBERSHIP;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        shouldParseUpdate5AddForumMembership(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate5AddForumMembershipWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_5_ADD_FORUM_MEMBERSHIP;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        shouldParseUpdate5AddForumMembership(writeEventStreamReader);
        charSeeker.close();
    }

    public void shouldParseUpdate5AddForumMembership(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate5AddForumMembership addForumMembership = (LdbcUpdate5AddForumMembership) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 4, 18, 42, 51);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addForumMembership.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addForumMembership.forumId(), is(2153L));
        assertThat(addForumMembership.personId(), is(372L));
        assertThat(addForumMembership.creationDate(), equalTo(creationDate));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate6AddPostWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_6_ADD_POST;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate6AddPost(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate6AddPostWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_6_ADD_POST;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate6AddPost(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate6AddPost(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 30, 7, 59, 58);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addPost.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPost.postId(), is(120343L));
        assertThat(addPost.imageFile(), equalTo(""));
        assertThat(addPost.creationDate(), equalTo(creationDate));
        assertThat(addPost.locationIp(), equalTo("91.229.229.89"));
        assertThat(addPost.browserUsed(), equalTo("Internet Explorer"));
        assertThat(addPost.language(), equalTo(""));
        assertThat(addPost.content(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
        assertThat(addPost.length(), is(172));
        assertThat(addPost.authorPersonId(), is(1673L));
        assertThat(addPost.forumId(), is(2152L));
        assertThat(addPost.countryId(), is(9L));
        assertThat(addPost.tagIds(), equalTo((List) Lists.newArrayList(1437l)));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate6AddPostWithEmptyTagsWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_6_ADD_POST_NO_TAGS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate6AddPostWithEmptyTags(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate6AddPostWithEmptyTagsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_6_ADD_POST_NO_TAGS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate6AddPostWithEmptyTags(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate6AddPostWithEmptyTags(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 30, 7, 59, 58);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addPost.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPost.postId(), is(120343L));
        assertThat(addPost.imageFile(), equalTo(""));
        assertThat(addPost.creationDate(), equalTo(creationDate));
        assertThat(addPost.locationIp(), equalTo("91.229.229.89"));
        assertThat(addPost.browserUsed(), equalTo("Internet Explorer"));
        assertThat(addPost.language(), equalTo(""));
        assertThat(addPost.content(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
        assertThat(addPost.length(), is(172));
        assertThat(addPost.authorPersonId(), is(1673L));
        assertThat(addPost.forumId(), is(2152L));
        assertThat(addPost.countryId(), is(9L));
        assertThat(addPost.tagIds(), equalTo((List) Lists.newArrayList()));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate7AddCommentWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_7_ADD_COMMENT;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate7AddComment(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate7AddCommentWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_7_ADD_COMMENT;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate7AddComment(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate7AddComment(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.JANUARY, 31, 23, 58, 49);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addComment.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addComment.commentId(), is(4034293L));
        assertThat(addComment.creationDate(), equalTo(creationDate));
        assertThat(addComment.locationIp(), equalTo("200.11.32.131"));
        assertThat(addComment.browserUsed(), equalTo("Firefox"));
        assertThat(addComment.content(), equalTo("words"));
        assertThat(addComment.length(), is(169));
        assertThat(addComment.authorPersonId(), is(7460L));
        assertThat(addComment.countryId(), is(91L));
        assertThat(addComment.replyToPostId(), is(-1L));
        assertThat(addComment.replyToCommentId(), is(4034289L));
        assertThat(addComment.tagIds(), equalTo((List) Lists.newArrayList(1403l, 1990l, 2009l, 2081l, 2817l, 2855l, 2987l, 6316l, 7425l, 8224l, 8466l)));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate7AddCommentWithEmptyTagsWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_7_ADD_COMMENT_NO_TAGS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate7AddCommentWithEmptyTags(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate7AddCommentWithEmptyTagsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_7_ADD_COMMENT_NO_TAGS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate7AddCommentWithEmptyTags(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate7AddCommentWithEmptyTags(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.JANUARY, 31, 23, 58, 49);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addComment.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addComment.commentId(), is(4034293L));
        assertThat(addComment.creationDate(), equalTo(creationDate));
        assertThat(addComment.locationIp(), equalTo("200.11.32.131"));
        assertThat(addComment.browserUsed(), equalTo("Firefox"));
        assertThat(addComment.content(), equalTo("words"));
        assertThat(addComment.length(), is(169));
        assertThat(addComment.authorPersonId(), is(7460L));
        assertThat(addComment.countryId(), is(91L));
        assertThat(addComment.replyToPostId(), is(-1L));
        assertThat(addComment.replyToCommentId(), is(4034289L));
        assertThat(addComment.tagIds(), equalTo((List) Lists.newArrayList()));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate8AddFriendshipWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_8_ADD_FRIENDSHIP;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW writeEventStreamReader = new WriteEventStreamReader_NEW(csvFileReader);
        doShouldParseUpdate8AddFriendship(writeEventStreamReader);
        csvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdate8AddFriendshipWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData_NEW.UPDATE_8_ADD_FRIENDSHIP;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW writeEventStreamReader = new WriteEventStreamReader_NEW_NEW(charSeeker, delimiters);
        doShouldParseUpdate8AddFriendship(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate8AddFriendship(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
        LdbcUpdate8AddFriendship addFriendship = (LdbcUpdate8AddFriendship) writeEventStreamReader.next();

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 10, 15, 58, 45);
        c.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date creationDate = c.getTime();

        assertThat(addFriendship.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addFriendship.person1Id(), is(1920L));
        assertThat(addFriendship.person2Id(), is(655L));
        assertThat(addFriendship.creationDate(), equalTo(creationDate));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdateEventFilesWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String forumCsvFilePath = TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath();
        File forumCsvFile = new File(forumCsvFilePath);
        SimpleCsvFileReader forumCsvFileReader = new SimpleCsvFileReader(forumCsvFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW forumWriteEventStreamReader = new WriteEventStreamReader_NEW(forumCsvFileReader);

        String personCsvFilePath = TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath();
        File personCsvFile = new File(personCsvFilePath);
        SimpleCsvFileReader personCsvFileReader = new SimpleCsvFileReader(personCsvFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW personWriteEventStreamReader = new WriteEventStreamReader_NEW(personCsvFileReader);

        doShouldParseUpdateEventFiles(forumWriteEventStreamReader, personWriteEventStreamReader);

        forumCsvFileReader.closeReader();
        personCsvFileReader.closeReader();
    }

    @Test
    public void shouldParseUpdateEventFilesWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String forumCsvFilePath = TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath();
        File forumCsvFile = new File(forumCsvFilePath);
        CharSeeker forumCharSeeker = new BufferedCharSeeker(new FileReader(forumCsvFile));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW forumWriteEventStreamReader = new WriteEventStreamReader_NEW_NEW(forumCharSeeker, delimiters);

        String personCsvFilePath = TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath();
        File personCsvFile = new File(personCsvFilePath);
        CharSeeker personCharSeeker = new BufferedCharSeeker(new FileReader(personCsvFile));
        WriteEventStreamReader_NEW_NEW personWriteEventStreamReader = new WriteEventStreamReader_NEW_NEW(personCharSeeker, delimiters);

        doShouldParseUpdateEventFiles(forumWriteEventStreamReader, personWriteEventStreamReader);

        forumCharSeeker.close();
        personCharSeeker.close();
    }

    public void doShouldParseUpdateEventFiles(Iterator<Operation<?>> forumWriteEventStreamReader, Iterator<Operation<?>> personWriteEventStreamReader) throws IOException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        Iterator<Class<?>> updateEventTypes = Iterators.transform(
                gf.mergeSortOperationsByStartTime(
                        forumWriteEventStreamReader,
                        personWriteEventStreamReader
                ),
                new Function<Operation<?>, Class<?>>() {
                    @Override
                    public Class<?> apply(Operation<?> input) {
                        return input.getClass();
                    }
                }
        );

        Histogram<Class<?>, Long> histogram = new Histogram<>(0L);
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate1AddPerson.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate2AddPostLike.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate3AddCommentLike.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate4AddForum.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate5AddForumMembership.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate6AddPost.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate7AddComment.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate8AddFriendship.class));

        Time startTime = timeSource.now();

        histogram.importValueSequence(updateEventTypes);

        Duration runtime = timeSource.now().durationGreaterThan(startTime);

        System.out.println(String.format("Runtime:\t\t%s", runtime));
        System.out.println(String.format("Operation count:\t%s", histogram.sumOfAllBucketValues()));
        System.out.println(String.format("Throughput (op/ms):\t%s", histogram.sumOfAllBucketValues() / runtime.asMilli()));
        System.out.println(histogram.toPercentageValues().toPrettyString());
    }

    @Test
    public void timestampsInUpdateStreamShouldBeMonotonicallyIncreasingWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String forumCsvFilePath = TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath();
        File forumCsvFile = new File(forumCsvFilePath);
        SimpleCsvFileReader forumCsvFileReader = new SimpleCsvFileReader(forumCsvFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW forumWriteEventStreamReader = new WriteEventStreamReader_NEW(forumCsvFileReader);

        String personCsvFilePath = TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath();
        File personCsvFile = new File(personCsvFilePath);
        SimpleCsvFileReader personCsvFileReader = new SimpleCsvFileReader(personCsvFile, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReader_NEW personWriteEventStreamReader = new WriteEventStreamReader_NEW(personCsvFileReader);

        doShouldParseUpdateEventFiles(forumWriteEventStreamReader, personWriteEventStreamReader);

        forumCsvFileReader.closeReader();
        personCsvFileReader.closeReader();
    }

    @Test
    public void timestampsInUpdateStreamShouldBeMonotonicallyIncreasingWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String forumCsvFilePath = TestUtils.getResource("/updateStream_0_0_forum.csv").getAbsolutePath();
        File forumCsvFile = new File(forumCsvFilePath);
        CharSeeker forumCharSeeker = new BufferedCharSeeker(new FileReader(forumCsvFile));
        int[] delimiters = new int[]{'|'};
        WriteEventStreamReader_NEW_NEW forumWriteEventStreamReader = new WriteEventStreamReader_NEW_NEW(forumCharSeeker, delimiters);

        String personCsvFilePath = TestUtils.getResource("/updateStream_0_0_person.csv").getAbsolutePath();
        File personCsvFile = new File(personCsvFilePath);
        CharSeeker personCharSeeker = new BufferedCharSeeker(new FileReader(personCsvFile));
        WriteEventStreamReader_NEW_NEW personWriteEventStreamReader = new WriteEventStreamReader_NEW_NEW(personCharSeeker, delimiters);

        doShouldParseUpdateEventFiles(forumWriteEventStreamReader, personWriteEventStreamReader);

        forumCharSeeker.close();
        personCharSeeker.close();
    }

    @Test
    public void doTimestampsInUpdateStreamShouldBeMonotonicallyIncreasing(Iterator<Operation<?>> forumWriteEventStreamReader, Iterator<Operation<?>> personWriteEventStreamReader) throws IOException {
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Iterator<Operation<?>> operations = gf.mergeSortOperationsByStartTime(
                forumWriteEventStreamReader,
                personWriteEventStreamReader
        );

        Time previousOperationTime = Time.fromMilli(0);
        while (operations.hasNext()) {
            Operation<?> writeOperation = operations.next();
            Time currentOperationTime = writeOperation.scheduledStartTime();
            assertThat(currentOperationTime.gte(previousOperationTime), is(true));
            previousOperationTime = currentOperationTime;
        }
    }
}
