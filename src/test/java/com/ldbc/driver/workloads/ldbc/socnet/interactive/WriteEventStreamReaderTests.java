package com.ldbc.driver.workloads.ldbc.socnet.interactive;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.ldbc.driver.Operation;
import com.ldbc.driver.generator.CsvEventStreamReader;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.temporal.Duration;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.util.Histogram;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.TestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;

import static com.ldbc.driver.util.Bucket.DiscreteBucket;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class WriteEventStreamReaderTests {
    File csvFile = null;

    @Before
    public void createCsvFile() throws IOException {
        csvFile = new File("temp.csv");
        csvFile.createNewFile();
    }

    @After
    public void deleteCsvFile() {
        FileUtils.deleteQuietly(csvFile);
    }

    @Test
    public void shouldParseUpdate1AddPerson() throws IOException, ParseException {
        // Given
        String jsonString = "[" +
                "409," +
                "\"Lei\"," +
                "\"Zhao\"," +
                "\"male\"," +
                "\"1989-07-21\"," +
                "\"2011-01-18T08:36:04Z\"," +
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
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_PERSON;
        LdbcUpdate1AddPerson addPerson = (LdbcUpdate1AddPerson) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(1989, Calendar.JULY, 21);
        Date birthday = c.getTime();
        c.clear();
        c.set(2011, Calendar.JANUARY, 18, 8, 36, 4);
        Date creationDate = c.getTime();

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
        String jsonString = "[1582,120207,\"2011-02-01T08:36:04Z\"]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST;
        LdbcUpdate2AddPostLike addPostLike = (LdbcUpdate2AddPostLike) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.FEBRUARY, 01, 8, 36, 04);
        Date creationDate = c.getTime();

        assertThat(addPostLike.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addPostLike.personId(), is(1582L));
        assertThat(addPostLike.postId(), is(120207L));
        assertThat(addPostLike.creationDate(), equalTo(creationDate));
    }

    @Test
    public void shouldParseUpdate3AddLikeComment() throws IOException, ParseException {
        // Given
        String jsonString = "[1095,120426,\"2011-01-24T05:44:13Z\"]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_COMMENT;
        LdbcUpdate3AddCommentLike addCommentLike = (LdbcUpdate3AddCommentLike) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 24, 5, 44, 13);
        Date creationDate = c.getTime();

        assertThat(addCommentLike.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addCommentLike.personId(), is(1095L));
        assertThat(addCommentLike.commentId(), is(120426L));
        assertThat(addCommentLike.creationDate(), equalTo(creationDate));
    }

    @Test
    public void shouldParseUpdate4AddForum() throws IOException, ParseException {
        // Given
        String jsonString = "[2118,\"Group for The_Beekeeper in Pakistan\",\"2011-01-03T06:04:47Z\",989,[10716]]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_FORUM;
        LdbcUpdate4AddForum addForum = (LdbcUpdate4AddForum) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 3, 6, 4, 47);
        Date creationDate = c.getTime();

        assertThat(addForum.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addForum.forumId(), is(2118L));
        assertThat(addForum.forumTitle(), equalTo("Group for The_Beekeeper in Pakistan"));
        assertThat(addForum.creationDate(), equalTo(creationDate));
        assertThat(addForum.moderatorPersonId(), is(989L));
        assertThat(addForum.tagIds(), equalTo(new long[]{10716}));
    }

    @Test
    public void shouldParseUpdate5AddForumMembership() throws IOException, ParseException {
        // Given
        String jsonString = "[2153,372,\"2011-01-04T18:42:51Z\"]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_FORUM_MEMBERSHIP;
        LdbcUpdate5AddForumMembership addForumMembership = (LdbcUpdate5AddForumMembership) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 4, 18, 42, 51);
        Date creationDate = c.getTime();

        assertThat(addForumMembership.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addForumMembership.forumId(), is(2153L));
        assertThat(addForumMembership.personId(), is(372L));
        assertThat(addForumMembership.creationDate(), equalTo(creationDate));
    }

    @Test
    public void shouldParseUpdate6AddPost() throws IOException, ParseException {
        // Given
        String jsonString = "[" +
                "120343," +
                "\"\"," +
                "\"2011-01-30T07:59:58Z\"," +
                "\"91.229.229.89\"," +
                "\"Internet Explorer\"," +
                "\"\"," +
                "\"About Venustiano Carranza, 1920) was one of the leaders of the Mexican Revolution. He ultimately became President of Mexico following the overthrow of the dictatorial Huer.\"," +
                "172," +
                "1673," +
                "2152," +
                "9," +
                "[1437]]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_POST;
        LdbcUpdate6AddPost addPost = (LdbcUpdate6AddPost) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 30, 7, 59, 58);
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
        assertThat(addPost.tagIds(), equalTo(new long[]{1437}));
    }

    @Test
    public void shouldParseUpdate7AddComment() throws IOException, ParseException {
        // Given
        String jsonString = "[" +
                "4034293," +
                "\"2013-01-31T23:58:49Z\"," +
                "\"200.11.32.131\"," +
                "\"Firefox\"," +
                "\"words\"," +
                "169," +
                "7460," +
                "91," +
                "-1," +
                "4034289," +
                "[1403,1990,2009,2081,2817,2855,2987,6316,7425,8224,8466]]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_COMMENT;
        LdbcUpdate7AddComment addComment = (LdbcUpdate7AddComment) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2013, Calendar.JANUARY, 31, 23, 58, 49);
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
        assertThat(addComment.tagIds(), equalTo(new long[]{1403, 1990, 2009, 2081, 2817, 2855, 2987, 6316, 7425, 8224, 8466}));
    }

    @Test
    public void shouldParseUpdate8AddFriendship() throws IOException, ParseException {
        // Given
        String jsonString = "[1920,655,\"2011-01-10T15:58:45Z\"]";

        // When
        CsvEventStreamReader.EventDecoder<Operation<?>> decoder = WriteEventStreamReader.EVENT_DECODER_ADD_FRIENDSHIP;
        LdbcUpdate8AddFriendship addFriendship = (LdbcUpdate8AddFriendship) decoder.decodeEvent(new String[]{"42", null, jsonString});

        // Then
        Calendar c = Calendar.getInstance();
        c.clear();
        c.set(2011, Calendar.JANUARY, 10, 15, 58, 45);
        Date creationDate = c.getTime();

        assertThat(addFriendship.scheduledStartTime(), is(Time.fromMilli(42)));
        assertThat(addFriendship.person1Id(), is(1920L));
        assertThat(addFriendship.person2Id(), is(655L));
        assertThat(addFriendship.creationDate(), equalTo(creationDate));
    }

    @Test
    public void shouldParseUpdateEventFileWithExactlyOneMatch() throws FileNotFoundException {
        String csvFilePath = TestUtils.getResource("/updateStream_0.csv").getAbsolutePath();
        File csvFile = new File(csvFilePath);
        CsvFileReader csvFileReader = new CsvFileReader(csvFile, "\\|");
        WriteEventStreamReader writeEventStreamReader = new WriteEventStreamReader(csvFileReader, CsvEventStreamReader.EventReturnPolicy.EXACTLY_ONE_MATCH);
        Iterator<Class<?>> updateEventTypes = Iterators.transform(writeEventStreamReader, new Function<Operation<?>, Class<?>>() {
            @Override
            public Class<?> apply(Operation<?> input) {
                return input.getClass();
            }
        });
        Histogram<Class<?>, Long> histogram = new Histogram<>(0L);
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate1AddPerson.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate2AddPostLike.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate3AddCommentLike.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate4AddForum.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate5AddForumMembership.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate6AddPost.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate7AddComment.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate8AddFriendship.class));

        Time startTime = Time.now();

        histogram.importValueSequence(updateEventTypes);

        Duration runtime = Time.now().greaterBy(startTime);

        System.out.println(String.format("Runtime:\t\t%s", runtime));
        System.out.println(String.format("Operation count:\t%s", histogram.sumOfAllBucketValues()));
        System.out.println(String.format("Throughput (op/ms):\t%s", histogram.sumOfAllBucketValues() / runtime.asMilli()));
        System.out.println(histogram.toPercentageValues().toPrettyString());
    }

    @Test
    public void shouldParseUpdateEventFileWithAtLeastOneMatch() throws FileNotFoundException {
        String csvFilePath = TestUtils.getResource("/updateStream_0.csv").getAbsolutePath();
        File csvFile = new File(csvFilePath);
        CsvFileReader csvFileReader = new CsvFileReader(csvFile, "\\|");
        WriteEventStreamReader writeEventStreamReader = new WriteEventStreamReader(csvFileReader, CsvEventStreamReader.EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Iterator<Class<?>> updateEventTypes = Iterators.transform(writeEventStreamReader, new Function<Operation<?>, Class<?>>() {
            @Override
            public Class<?> apply(Operation<?> input) {
                return input.getClass();
            }
        });
        Histogram<Class<?>, Long> histogram = new Histogram<>(0L);
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate1AddPerson.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate2AddPostLike.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate3AddCommentLike.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate4AddForum.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate5AddForumMembership.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate6AddPost.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate7AddComment.class));
        histogram.addBucket(DiscreteBucket.<Class<?>>create(LdbcUpdate8AddFriendship.class));

        Time startTime = Time.now();

        histogram.importValueSequence(updateEventTypes);

        Duration runtime = Time.now().greaterBy(startTime);

        System.out.println(String.format("Runtime:\t\t%s", runtime));
        System.out.println(String.format("Operation count:\t%s", histogram.sumOfAllBucketValues()));
        System.out.println(String.format("Throughput (op/ms):\t%s", histogram.sumOfAllBucketValues() / runtime.asMilli()));
        System.out.println(histogram.toPercentageValues().toPrettyString());
    }

    @Test
    public void timestampsInUpdateStreamShouldBeMonotonicallyIncreasing() throws FileNotFoundException {
        String csvFilePath = TestUtils.getResource("/updateStream_0.csv").getAbsolutePath();
        File csvFile = new File(csvFilePath);
        CsvFileReader csvFileReader = new CsvFileReader(csvFile, "\\|");
        WriteEventStreamReader writeEventStreamReader = new WriteEventStreamReader(csvFileReader, CsvEventStreamReader.EventReturnPolicy.AT_LEAST_ONE_MATCH);
        Time previousOperationTime = Time.fromMilli(0);
        while (writeEventStreamReader.hasNext()) {
            Operation<?> writeOperation = writeEventStreamReader.next();
            Time currentOperationTime = writeOperation.scheduledStartTime();
            assertThat(currentOperationTime.gte(previousOperationTime), is(true));
            previousOperationTime = currentOperationTime;
        }
    }

    @Test
    public void timestampsInUpdateStreamShouldBeMonotonicallyIncreasingAfterOffset() throws FileNotFoundException {
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        String csvFilePath = TestUtils.getResource("/updateStream_0.csv").getAbsolutePath();
        File csvFile = new File(csvFilePath);
        CsvFileReader csvFileReader = new CsvFileReader(csvFile, "\\|");
        Iterator<Operation<?>> writeEventStreamReader = generators.timeOffset(new WriteEventStreamReader(csvFileReader, CsvEventStreamReader.EventReturnPolicy.AT_LEAST_ONE_MATCH), Time.now());
        Time previousOperationTime = Time.fromMilli(0);
        while (writeEventStreamReader.hasNext()) {
            Operation<?> writeOperation = writeEventStreamReader.next();
            Time currentOperationTime = writeOperation.scheduledStartTime();
            assertThat(currentOperationTime.gte(previousOperationTime), is(true));
            previousOperationTime = currentOperationTime;
        }
    }
}
