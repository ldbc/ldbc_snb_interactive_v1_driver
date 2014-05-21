package com.ldbc.driver.generator;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.temporal.Time;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import com.ldbc.driver.util.TestUtils;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcUpdate2AddPostLike;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcUpdate5AddForumMembership;
import com.ldbc.driver.workloads.ldbc.socnet.interactive.LdbcUpdate7AddComment;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static com.ldbc.driver.generator.CsvEventStreamReader.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CsvEventStreamReaderTest {

    GeneratorFactory generators;
    File eventStreamCsvFile;
    ObjectMapper objectMapper = new ObjectMapper();
    String DATE_TIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss'Z'";
    SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat(DATE_TIME_FORMAT_STRING);

    EventDecoder<Operation<?>> EVENT_DECODER_ADD_POST_LIKE = new EventDecoder<Operation<?>>() {
        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_LIKE_POST");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long personId = params.get(0).asLong();
            long postId = params.get(1).asLong();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate2AddPostLike(personId, postId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    EventDecoder<Operation<?>> EVENT_DECODER_ADD_COMMENT = new EventDecoder<Operation<?>>() {
        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_COMMENT");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long commentId = params.get(0).asLong();
            String creationDateString = params.get(1).asText();
            Date creationDate;
            try {
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            String locationIp = params.get(2).asText();
            String browserUsed = params.get(3).asText();
            String content = params.get(4).asText();
            int length = params.get(5).asInt();
            long authorPersonId = params.get(6).asLong();
            long countryId = params.get(7).asLong();
            long replyOfPostId = params.get(8).asLong();
            long replyOfCommentId = params.get(9).asLong();
            List<Long> tagIdsList = Lists.newArrayList(Iterables.transform(params.get(10), new Function<JsonNode, Long>() {
                @Override
                public Long apply(JsonNode input) {
                    return input.asLong();
                }
            }));
            Operation<?> operation = new LdbcUpdate7AddComment(
                    commentId,
                    creationDate,
                    locationIp,
                    browserUsed,
                    content,
                    length,
                    authorPersonId,
                    countryId,
                    replyOfPostId,
                    replyOfCommentId,
                    longListToPrimitiveLongArray(tagIdsList));
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    EventDecoder<Operation<?>> EVENT_DECODER_ADD_FORUM_MEMBERSHIP = new EventDecoder<Operation<?>>() {
        @Override
        public boolean eventMatchesDecoder(String[] csvRow) {
            return csvRow[1].equals("ADD_FORUM_MEMBERSHIP");
        }

        @Override
        public Operation<?> decodeEvent(String[] csvRow) {
            Time eventDueTime = Time.fromMilli(Long.parseLong(csvRow[0]));
            String eventParamsAsJsonString = csvRow[2];
            JsonNode params;
            try {
                params = objectMapper.readTree(eventParamsAsJsonString);
            } catch (IOException e) {
                throw new GeneratorException(String.format("Error parsing JSON event params\n%s", eventParamsAsJsonString), e);
            }
            long forumId = params.get(0).asLong();
            long personId = params.get(1).asLong();
            String creationDateString = params.get(2).asText();
            Date creationDate;
            try {
                creationDate = DATE_TIME_FORMAT.parse(creationDateString);
            } catch (ParseException e) {
                throw new GeneratorException(String.format("Error parsing creation date string\n%s", creationDateString), e);
            }
            Operation<?> operation = new LdbcUpdate5AddForumMembership(forumId, personId, creationDate);
            operation.setScheduledStartTime(eventDueTime);
            return operation;
        }
    };

    @Before
    public void initGenerators() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        eventStreamCsvFile = TestUtils.getResource("/test_events.csv");
    }

    long[] longListToPrimitiveLongArray(List<Long> longList) {
        long[] longArray = new long[longList.size()];
        for (int i = 0; i < longList.size(); i++)
            longArray[i] = longList.get(i);
        return longArray;
    }

    @Test
    public void shouldParseEntireFileWhenAllDecodersAreProvided() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/test_events.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                EVENT_DECODER_ADD_POST_LIKE,
                EVENT_DECODER_ADD_COMMENT,
                EVENT_DECODER_ADD_FORUM_MEMBERSHIP);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // When
        CsvEventStreamReader<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(file, eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate7AddComment.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldFailToParseEntireFileWhenOneDecoderNotProvidedAndAtLeastOnePolicy() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/test_events.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                EVENT_DECODER_ADD_POST_LIKE,
                EVENT_DECODER_ADD_COMMENT);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // When
        CsvEventStreamReader<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(file, eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate7AddComment.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        boolean exceptionThrown = false;
        try {
            csvEventStreamReader.next();
        } catch (GeneratorException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldFailToParseEntireFileWhenOneDecoderNotProvidedAndExactlyOnePolicy() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/test_events.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                EVENT_DECODER_ADD_POST_LIKE,
                EVENT_DECODER_ADD_COMMENT);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.EXACTLY_ONE_MATCH);

        // When
        CsvEventStreamReader<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(file, eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate7AddComment.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        boolean exceptionThrown = false;
        try {
            csvEventStreamReader.next();
        } catch (GeneratorException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void shouldParseEntireFileWhenMultipleDecodersMatchSameEventAndAtLeastOncePolicy() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/test_events.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                EVENT_DECODER_ADD_POST_LIKE,
                EVENT_DECODER_ADD_COMMENT,
                EVENT_DECODER_ADD_COMMENT,
                EVENT_DECODER_ADD_FORUM_MEMBERSHIP);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // When
        CsvEventStreamReader<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(file, eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate7AddComment.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldFailToParseEntireFileWhenMultipleDecodersMatchSameEventAndExactlyOncePolicy() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/test_events.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                EVENT_DECODER_ADD_POST_LIKE,
                EVENT_DECODER_ADD_COMMENT,
                EVENT_DECODER_ADD_COMMENT);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.EXACTLY_ONE_MATCH);

        // When
        CsvEventStreamReader<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(file, eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        boolean exceptionThrown = false;
        try {
            csvEventStreamReader.next();
        } catch (GeneratorException e) {
            exceptionThrown = true;
        }
        assertThat(exceptionThrown, is(true));
    }
}
