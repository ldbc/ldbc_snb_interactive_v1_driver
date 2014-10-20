package com.ldbc.driver.workloads.ldbc.snb.interactive;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.csv.BufferedCharSeeker;
import com.ldbc.driver.util.csv.CharSeeker;
import com.ldbc.driver.util.csv.Extractors;
import com.ldbc.driver.util.csv.SimpleCsvFileReader;
import org.junit.Ignore;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WriteEventStreamReaderTest {
    @Test
    public void shouldParseAllEventTypesWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.ROWS_FOR_ALL_EVENT_TYPES;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseAllEventTypes(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseAllEventTypesWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.ROWS_FOR_ALL_EVENT_TYPES;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPerson(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
    public void shouldParseUpdate1AddPersonWithOneLanguageWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_ONE_LANGUAGE;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithOneLanguage(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithOneLanguageWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_ONE_LANGUAGE;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithOneLanguage(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithOneLanguage(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPerson.languages(), equalTo((List) Lists.newArrayList("swedish")));
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
    public void shouldParseUpdate1AddPersonWithNoLanguagesWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_LANGUAGES;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithNoLanguages(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoLanguagesWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_LANGUAGES;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithNoLanguages(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithNoLanguages(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPerson.languages(), equalTo((List) Lists.<String>newArrayList()));
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
    public void shouldParseUpdate1AddPersonWithOneCompanyWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_ONE_COMPANY;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithOneCompany(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithOneCompanyWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_ONE_COMPANY;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithOneCompany(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithOneCompany(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
                new LdbcUpdate1AddPerson.Organization(911L, 1970)
        )));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoCompaniesWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_COMPANIES;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithNoCompanies(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoCompaniesWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_COMPANIES;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithNoCompanies(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithNoCompanies(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPerson.workAt(), equalTo((List) Lists.newArrayList()));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoUnisWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_UNIS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithNoUnis(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoUnisWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_UNIS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithNoUnis(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithNoUnis(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPerson.studyAt(), equalTo((List) Lists.newArrayList()));
        assertThat(addPerson.workAt(), equalTo((List) Lists.newArrayList(
                new LdbcUpdate1AddPerson.Organization(911L, 1970),
                new LdbcUpdate1AddPerson.Organization(935L, 1970),
                new LdbcUpdate1AddPerson.Organization(913L, 1971),
                new LdbcUpdate1AddPerson.Organization(1539L, 1971)
        )));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoEmailsWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_EMAILS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithNoEmails(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate1AddPersonWithNoEmailsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_EMAILS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithNoEmails(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithNoEmails(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPerson.emails(), equalTo((List) Lists.newArrayList()));
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
    public void shouldParseUpdate1AddPersonWithNoTagsWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_TAGS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate1AddPersonWithNoTags(writeEventStreamReader);
        csvFileReader.close();
    }

    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    // TODO unignore once latest neo4j-csv is available from maven
    @Ignore
    @Test
    public void shouldParseUpdate1AddPersonWithNoTagsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_TAGS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate1AddPersonWithNoTags(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate1AddPersonWithNoTags(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPerson.tagIds(), equalTo((List) Lists.newArrayList()));
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
        String data = WriteEventStreamReaderTestData.UPDATE_2_ADD_LIKE_POST_ROW;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate2AddLikePost(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate2AddLikePostWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        // Given
        String data = WriteEventStreamReaderTestData.UPDATE_2_ADD_LIKE_POST_ROW;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_3_ADD_LIKE_COMMENT;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate3AddLikeComment(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate3AddLikeCommentWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_3_ADD_LIKE_COMMENT;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_4_ADD_FORUM;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate4AddForum(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate4AddForumWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_4_ADD_FORUM;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_5_ADD_FORUM_MEMBERSHIP;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        shouldParseUpdate5AddForumMembership(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate5AddForumMembershipWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_5_ADD_FORUM_MEMBERSHIP;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_6_ADD_POST;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate6AddPost(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate6AddPostWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_6_ADD_POST;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
    public void shouldParseUpdate6AddPostWithManyTagsWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_6_ADD_POST_MANY_TAGS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate6AddPostWithManyTags(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate6AddPostWithManyTagsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_6_ADD_POST_MANY_TAGS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
        doShouldParseUpdate6AddPostWithManyTags(writeEventStreamReader);
        charSeeker.close();
    }

    public void doShouldParseUpdate6AddPostWithManyTags(Iterator<Operation<?>> writeEventStreamReader) throws IOException, ParseException {
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
        assertThat(addPost.tagIds(), equalTo((List) Lists.newArrayList(1437l, 167l, 182l)));
        assertThat(writeEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldParseUpdate6AddPostWithEmptyTagsWithWriteEventStreamReader_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_6_ADD_POST_NO_TAGS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate6AddPostWithEmptyTags(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate6AddPostWithEmptyTagsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_6_ADD_POST_NO_TAGS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_7_ADD_COMMENT;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate7AddComment(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate7AddCommentWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_7_ADD_COMMENT;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_7_ADD_COMMENT_NO_TAGS;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate7AddCommentWithEmptyTags(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate7AddCommentWithEmptyTagsWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_7_ADD_COMMENT_NO_TAGS;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
        String data = WriteEventStreamReaderTestData.UPDATE_8_ADD_FRIENDSHIP;
        BufferedReader bufferedReader = new BufferedReader(new StringReader(data));
        SimpleCsvFileReader csvFileReader = new SimpleCsvFileReader(bufferedReader, SimpleCsvFileReader.DEFAULT_COLUMN_SEPARATOR_PATTERN);
        WriteEventStreamReaderRegex writeEventStreamReader = new WriteEventStreamReaderRegex(csvFileReader);
        doShouldParseUpdate8AddFriendship(writeEventStreamReader);
        csvFileReader.close();
    }

    @Test
    public void shouldParseUpdate8AddFriendshipWithWriteEventStreamReader_NEW_NEW() throws IOException, ParseException {
        String data = WriteEventStreamReaderTestData.UPDATE_8_ADD_FRIENDSHIP;
        CharSeeker charSeeker = new BufferedCharSeeker(new StringReader(data));
        int columnDelimiter = '|';
        Extractors extractors = new Extractors(';');
        WriteEventStreamReaderCharSeeker writeEventStreamReader = new WriteEventStreamReaderCharSeeker(charSeeker, extractors, columnDelimiter);
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
}
