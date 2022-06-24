// package org.ldbcouncil.snb.driver.workloads.interactive;

// import com.google.common.collect.Lists;
// import org.ldbcouncil.snb.driver.Operation;
// import org.ldbcouncil.snb.driver.csv.charseeker.BufferedCharSeeker;
// import org.ldbcouncil.snb.driver.csv.charseeker.CharSeeker;
// import org.ldbcouncil.snb.driver.csv.charseeker.Extractors;
// import org.ldbcouncil.snb.driver.csv.charseeker.Readables;
// import org.ldbcouncil.snb.driver.workloads.interactive.queries.*;
// import org.junit.Test;

// import java.io.IOException;
// import java.io.StringReader;
// import java.text.ParseException;
// import java.util.Date;
// import java.util.Iterator;
// import java.util.List;

// import static org.hamcrest.CoreMatchers.equalTo;
// import static org.hamcrest.CoreMatchers.is;
// import static org.junit.Assert.assertThat;

// public class InteractiveWriteEventStreamReaderTest
// {
//     @Test
//     public void shouldParseAllEventTypesWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.ROWS_FOR_ALL_EVENT_TYPES;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseAllEventTypes(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseAllEventTypes(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         // Then
//         Date birthday;
//         Date creationDate;

//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();
//         birthday = new Date(1234567890l);
//         creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         // OperationTest.assertCorrectParameterMap(addPerson);

//         LdbcUpdate2AddPostLike addPostLike = (LdbcUpdate2AddPostLike) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addPostLike.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPostLike.timeStamp(), is(42l));
//         assertThat(addPostLike.dependencyTimeStamp(), is(666l));
//         assertThat(addPostLike.getPersonId(), is(1582L));
//         assertThat(addPostLike.getPostId(), is(120207L));
//         assertThat(addPostLike.getCreationDate(), equalTo(creationDate));
//         // OperationTest.assertCorrectParameterMap(addPostLike);

//         LdbcUpdate3AddCommentLike addCommentLike = (LdbcUpdate3AddCommentLike) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addCommentLike.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addCommentLike.timeStamp(), is(42l));
//         assertThat(addCommentLike.dependencyTimeStamp(), is(666l));
//         assertThat(addCommentLike.getPersonId(), is(1095L));
//         assertThat(addCommentLike.getCommentId(), is(120426L));
//         assertThat(addCommentLike.getCreationDate(), equalTo(creationDate));
//         // OperationTest.assertCorrectParameterMap(addCommentLike);

//         LdbcUpdate4AddForum addForum = (LdbcUpdate4AddForum) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addForum.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addForum.timeStamp(), is(42l));
//         assertThat(addForum.dependencyTimeStamp(), is(666l));
//         assertThat(addForum.getForumId(), is(2118L));
//         assertThat(addForum.getForumTitle(), equalTo("Group for The_Beekeeper in Pakistan"));
//         assertThat(addForum.getCreationDate(), equalTo(creationDate));
//         assertThat(addForum.getModeratorPersonId(), is(989L));
//         assertThat(addForum.getTagIds(), equalTo((List) Lists.newArrayList(10716l)));
//         // OperationTest.assertCorrectParameterMap(addForum);

//         LdbcUpdate5AddForumMembership addForumMembership = (LdbcUpdate5AddForumMembership) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addForumMembership.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addForumMembership.timeStamp(), is(42l));
//         assertThat(addForumMembership.dependencyTimeStamp(), is(666l));
//         assertThat(addForumMembership.getForumId(), is(2153L));
//         assertThat(addForumMembership.getPersonId(), is(372L));
//         assertThat(addForumMembership.getJoinDate(), equalTo(creationDate));
//         // OperationTest.assertCorrectParameterMap(addForumMembership);

//         LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addPost.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPost.timeStamp(), is(42l));
//         assertThat(addPost.dependencyTimeStamp(), is(666l));
//         assertThat(addPost.getPostId(), is(120343L));
//         assertThat(addPost.getImageFile(), equalTo(""));
//         assertThat(addPost.getCreationDate(), equalTo(creationDate));
//         assertThat(addPost.getLocationIp(), equalTo("91.229.229.89"));
//         assertThat(addPost.getBrowserUsed(), equalTo("Internet Explorer"));
//         assertThat(addPost.getLanguage(), equalTo(""));
//         assertThat(addPost.getContent(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
//         assertThat(addPost.getLength(), is(172));
//         assertThat(addPost.getAuthorPersonId(), is(1673L));
//         assertThat(addPost.getForumId(), is(2152L));
//         assertThat(addPost.getCountryId(), is(9L));
//         assertThat(addPost.getTagIds(), equalTo((List) Lists.newArrayList(1437l)));
//         // OperationTest.assertCorrectParameterMap(addPost);

//         LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addComment.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addComment.timeStamp(), is(42l));
//         assertThat(addComment.dependencyTimeStamp(), is(666l));
//         assertThat(addComment.getCommentId(), is(4034293L));
//         assertThat(addComment.getCreationDate(), equalTo(creationDate));
//         assertThat(addComment.getLocationIp(), equalTo("200.11.32.131"));
//         assertThat(addComment.getBrowserUsed(), equalTo("Firefox"));
//         assertThat(addComment.getContent(), equalTo("words"));
//         assertThat(addComment.getLength(), is(169));
//         assertThat(addComment.getAuthorPersonId(), is(7460L));
//         assertThat(addComment.getCountryId(), is(91L));
//         assertThat(addComment.getReplyToPostId(), is(-1L));
//         assertThat(addComment.getReplyToCommentId(), is(4034289L));
//         assertThat(addComment.getTagIds(), equalTo((List) Lists.newArrayList(1403l, 1990l, 2009l, 2081l, 2817l, 2855l, 2987l, 6316l, 7425l, 8224l, 8466l)));
//         // OperationTest.assertCorrectParameterMap(addComment);

//         addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addComment.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addComment.timeStamp(), is(42l));
//         assertThat(addComment.dependencyTimeStamp(), is(666l));
//         assertThat(addComment.getCommentId(), is(4034293L));
//         assertThat(addComment.getCreationDate(), equalTo(creationDate));
//         assertThat(addComment.getLocationIp(), equalTo("200.11.32.131"));
//         assertThat(addComment.getBrowserUsed(), equalTo("Firefox"));
//         assertThat(addComment.getContent(), equalTo("words"));
//         assertThat(addComment.getLength(), is(169));
//         assertThat(addComment.getAuthorPersonId(), is(7460L));
//         assertThat(addComment.getCountryId(), is(91L));
//         assertThat(addComment.getReplyToPostId(), is(-1L));
//         assertThat(addComment.getReplyToCommentId(), is(4034289L));
//         assertThat(addComment.getTagIds(), equalTo((List) Lists.newArrayList()));
//         // OperationTest.assertCorrectParameterMap(addComment);

//         LdbcUpdate8AddFriendship addFriendship = (LdbcUpdate8AddFriendship) writeEventStreamReader.next();
//         creationDate = new Date(1234567890l);

//         assertThat(addFriendship.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addFriendship.timeStamp(), is(42l));
//         assertThat(addFriendship.dependencyTimeStamp(), is(666l));
//         assertThat(addFriendship.getPerson1Id(), is(1920L));
//         assertThat(addFriendship.getPerson2Id(), is(655L));
//         assertThat(addFriendship.getCreationDate(), equalTo(creationDate));
//         // OperationTest.assertCorrectParameterMap(addFriendship);

//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPerson(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPerson(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithOneLanguageWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_ONE_LANGUAGE;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithOneLanguage(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithOneLanguage(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithNoLanguagesWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_LANGUAGES;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithNoLanguages(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithNoLanguages(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.<String>newArrayList()));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithOneCompanyWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_ONE_COMPANY;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithOneCompany(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithOneCompany(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithNoCompaniesWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_COMPANIES;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithNoCompanies(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithNoCompanies(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList()));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithNoUnisWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_UNIS;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithNoUnis(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithNoUnis(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList()));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithNoEmailsWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_EMAILS;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithNoEmails(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithNoEmails(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList()));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList(1612L)));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate1AddPersonWithNoTagsWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_1_ADD_PERSON_ROW_NO_TAGS;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate1AddPersonWithNoTags(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate1AddPersonWithNoTags(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) writeEventStreamReader.next();

//         // Then
//         Date birthday = new Date(1234567890l);
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPerson.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPerson.timeStamp(), is(42l));
//         assertThat(addPerson.dependencyTimeStamp(), is(666l));
//         assertThat(addPerson.getPersonId(), is(409L));
//         assertThat(addPerson.getPersonFirstName(), equalTo("Lei"));
//         assertThat(addPerson.getPersonLastName(), equalTo("Zhao"));
//         assertThat(addPerson.getGender(), equalTo("male"));
//         assertThat(addPerson.getBirthday(), equalTo(birthday));
//         assertThat(addPerson.getCreationDate(), equalTo(creationDate));
//         assertThat(addPerson.getLocationIp(), equalTo("14.131.98.220"));
//         assertThat(addPerson.getBrowserUsed(), equalTo("Chrome"));
//         assertThat(addPerson.getCityId(), is(392L));
//         assertThat(addPerson.getLanguages(), equalTo((List) Lists.newArrayList("english", "swedish")));
//         assertThat(addPerson.getEmails(), equalTo((List) Lists.newArrayList("user@email.com")));
//         assertThat(addPerson.getTagIds(), equalTo((List) Lists.newArrayList()));
//         assertThat(addPerson.getStudyAt(), equalTo((List) Lists.newArrayList(new LdbcUpdate1AddPerson.Organization(97L, 1))));
//         assertThat(addPerson.getWorkAt(), equalTo((List) Lists.newArrayList(
//                 new LdbcUpdate1AddPerson.Organization(911L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(935L, 1970),
//                 new LdbcUpdate1AddPerson.Organization(913L, 1971),
//                 new LdbcUpdate1AddPerson.Organization(1539L, 1971)
//         )));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate2AddLikePostWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         // Given
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_2_ADD_LIKE_POST_ROW;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate2AddLikePost(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate2AddLikePost(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate2AddPostLike addPostLike = (LdbcUpdate2AddPostLike) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPostLike.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPostLike.timeStamp(), is(42l));
//         assertThat(addPostLike.dependencyTimeStamp(), is(666l));
//         assertThat(addPostLike.getPersonId(), is(1582L));
//         assertThat(addPostLike.getPostId(), is(120207L));
//         assertThat(addPostLike.getCreationDate(), equalTo(creationDate));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate3AddLikeCommentWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_3_ADD_LIKE_COMMENT;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate3AddLikeComment(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate3AddLikeComment(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate3AddCommentLike addCommentLike = (LdbcUpdate3AddCommentLike) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addCommentLike.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addCommentLike.timeStamp(), is(42l));
//         assertThat(addCommentLike.dependencyTimeStamp(), is(666l));
//         assertThat(addCommentLike.getPersonId(), is(1095L));
//         assertThat(addCommentLike.getCommentId(), is(120426L));
//         assertThat(addCommentLike.getCreationDate(), equalTo(creationDate));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate4AddForumWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_4_ADD_FORUM;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate4AddForum(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate4AddForum(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate4AddForum addForum = (LdbcUpdate4AddForum) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addForum.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addForum.timeStamp(), is(42l));
//         assertThat(addForum.dependencyTimeStamp(), is(666l));
//         assertThat(addForum.getForumId(), is(2118L));
//         assertThat(addForum.getForumTitle(), equalTo("Group for The_Beekeeper in Pakistan"));
//         assertThat(addForum.getCreationDate(), equalTo(creationDate));
//         assertThat(addForum.getModeratorPersonId(), is(989L));
//         assertThat(addForum.getTagIds(), equalTo((List) Lists.newArrayList(10716l)));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate5AddForumMembershipWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_5_ADD_FORUM_MEMBERSHIP;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         shouldParseUpdate5AddForumMembership(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void shouldParseUpdate5AddForumMembership(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate5AddForumMembership addForumMembership = (LdbcUpdate5AddForumMembership) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addForumMembership.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addForumMembership.timeStamp(), is(42l));
//         assertThat(addForumMembership.dependencyTimeStamp(), is(666l));
//         assertThat(addForumMembership.getForumId(), is(2153L));
//         assertThat(addForumMembership.getPersonId(), is(372L));
//         assertThat(addForumMembership.getJoinDate(), equalTo(creationDate));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate6AddPostWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_6_ADD_POST;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate6AddPost(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate6AddPost(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPost.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPost.timeStamp(), is(42l));
//         assertThat(addPost.dependencyTimeStamp(), is(666l));
//         assertThat(addPost.getPostId(), is(120343L));
//         assertThat(addPost.getImageFile(), equalTo(""));
//         assertThat(addPost.getCreationDate(), equalTo(creationDate));
//         assertThat(addPost.getLocationIp(), equalTo("91.229.229.89"));
//         assertThat(addPost.getBrowserUsed(), equalTo("Internet Explorer"));
//         assertThat(addPost.getLanguage(), equalTo(""));
//         assertThat(addPost.getContent(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
//         assertThat(addPost.getLength(), is(172));
//         assertThat(addPost.getAuthorPersonId(), is(1673L));
//         assertThat(addPost.getForumId(), is(2152L));
//         assertThat(addPost.getCountryId(), is(9L));
//         assertThat(addPost.getTagIds(), equalTo((List) Lists.newArrayList(1437l)));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate6AddPostWithManyTagsWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_6_ADD_POST_MANY_TAGS;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate6AddPostWithManyTags(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate6AddPostWithManyTags(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPost.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPost.timeStamp(), is(42l));
//         assertThat(addPost.dependencyTimeStamp(), is(666l));
//         assertThat(addPost.getPostId(), is(120343L));
//         assertThat(addPost.getImageFile(), equalTo(""));
//         assertThat(addPost.getCreationDate(), equalTo(creationDate));
//         assertThat(addPost.getLocationIp(), equalTo("91.229.229.89"));
//         assertThat(addPost.getBrowserUsed(), equalTo("Internet Explorer"));
//         assertThat(addPost.getLanguage(), equalTo(""));
//         assertThat(addPost.getContent(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
//         assertThat(addPost.getLength(), is(172));
//         assertThat(addPost.getAuthorPersonId(), is(1673L));
//         assertThat(addPost.getForumId(), is(2152L));
//         assertThat(addPost.getCountryId(), is(9L));
//         assertThat(addPost.getTagIds(), equalTo((List) Lists.newArrayList(1437l, 167l, 182l)));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate6AddPostWithEmptyTagsWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_6_ADD_POST_NO_TAGS;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate6AddPostWithEmptyTags(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate6AddPostWithEmptyTags(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addPost.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addPost.timeStamp(), is(42l));
//         assertThat(addPost.dependencyTimeStamp(), is(666l));
//         assertThat(addPost.getPostId(), is(120343L));
//         assertThat(addPost.getImageFile(), equalTo(""));
//         assertThat(addPost.getCreationDate(), equalTo(creationDate));
//         assertThat(addPost.getLocationIp(), equalTo("91.229.229.89"));
//         assertThat(addPost.getBrowserUsed(), equalTo("Internet Explorer"));
//         assertThat(addPost.getLanguage(), equalTo(""));
//         assertThat(addPost.getContent(), equalTo("About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer."));
//         assertThat(addPost.getLength(), is(172));
//         assertThat(addPost.getAuthorPersonId(), is(1673L));
//         assertThat(addPost.getForumId(), is(2152L));
//         assertThat(addPost.getCountryId(), is(9L));
//         assertThat(addPost.getTagIds(), equalTo((List) Lists.newArrayList()));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate7AddCommentWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_7_ADD_COMMENT;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate7AddComment(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate7AddComment(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addComment.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addComment.timeStamp(), is(42l));
//         assertThat(addComment.dependencyTimeStamp(), is(666l));
//         assertThat(addComment.getCommentId(), is(4034293L));
//         assertThat(addComment.getCreationDate(), equalTo(creationDate));
//         assertThat(addComment.getLocationIp(), equalTo("200.11.32.131"));
//         assertThat(addComment.getBrowserUsed(), equalTo("Firefox"));
//         assertThat(addComment.getContent(), equalTo("words"));
//         assertThat(addComment.getLength(), is(169));
//         assertThat(addComment.getAuthorPersonId(), is(7460L));
//         assertThat(addComment.getCountryId(), is(91L));
//         assertThat(addComment.getReplyToPostId(), is(-1L));
//         assertThat(addComment.getReplyToCommentId(), is(4034289L));
//         assertThat(addComment.getTagIds(), equalTo((List) Lists.newArrayList(1403l, 1990l, 2009l, 2081l, 2817l, 2855l, 2987l, 6316l, 7425l, 8224l, 8466l)));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate7AddCommentWithEmptyTagsWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_7_ADD_COMMENT_NO_TAGS;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate7AddCommentWithEmptyTags(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate7AddCommentWithEmptyTags(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addComment.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addComment.timeStamp(), is(42l));
//         assertThat(addComment.dependencyTimeStamp(), is(666l));
//         assertThat(addComment.getCommentId(), is(4034293L));
//         assertThat(addComment.getCreationDate(), equalTo(creationDate));
//         assertThat(addComment.getLocationIp(), equalTo("200.11.32.131"));
//         assertThat(addComment.getBrowserUsed(), equalTo("Firefox"));
//         assertThat(addComment.getContent(), equalTo("words"));
//         assertThat(addComment.getLength(), is(169));
//         assertThat(addComment.getAuthorPersonId(), is(7460L));
//         assertThat(addComment.getCountryId(), is(91L));
//         assertThat(addComment.getReplyToPostId(), is(-1L));
//         assertThat(addComment.getReplyToCommentId(), is(4034289L));
//         assertThat(addComment.getTagIds(), equalTo((List) Lists.newArrayList()));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }

//     @Test
//     public void shouldParseUpdate8AddFriendshipWithWriteEventStreamReaderCharSeeker_DATE() throws IOException, ParseException {
//         String data = InteractiveWriteEventStreamReaderTestData.UPDATE_8_ADD_FRIENDSHIP;
//         CharSeeker charSeeker = new BufferedCharSeeker(Readables.wrap(new StringReader(data)));
//         int columnDelimiter = '|';
//         Extractors extractors = new Extractors(';', ',');
//         Iterator<Operation> writeEventStreamReader = WriteEventStreamReaderCharSeeker.create(charSeeker, extractors, columnDelimiter);
//         doShouldParseUpdate8AddFriendship(writeEventStreamReader);
//         charSeeker.close();
//     }

//     public void doShouldParseUpdate8AddFriendship(Iterator<Operation> writeEventStreamReader) throws IOException, ParseException {
//         LdbcUpdate8AddFriendship addFriendship = (LdbcUpdate8AddFriendship) writeEventStreamReader.next();

//         // Then
//         Date creationDate = new Date(1234567890l);

//         assertThat(addFriendship.scheduledStartTimeAsMilli(), is(42l));
//         assertThat(addFriendship.timeStamp(), is(42l));
//         assertThat(addFriendship.dependencyTimeStamp(), is(666l));
//         assertThat(addFriendship.getPerson1Id(), is(1920L));
//         assertThat(addFriendship.getPerson2Id(), is(655L));
//         assertThat(addFriendship.getCreationDate(), equalTo(creationDate));
//         assertThat(writeEventStreamReader.hasNext(), is(false));
//     }
// }
