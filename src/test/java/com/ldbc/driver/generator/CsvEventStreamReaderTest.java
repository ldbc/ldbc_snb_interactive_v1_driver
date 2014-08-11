package com.ldbc.driver.generator;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import com.ldbc.driver.util.CsvFileReader;
import com.ldbc.driver.testutils.TestUtils;
import com.ldbc.driver.workloads.ldbc.snb.interactive.*;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Iterator;

import static com.ldbc.driver.generator.CsvEventStreamReader.*;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CsvEventStreamReaderTest {
    GeneratorFactory generators;
    File eventStreamCsvFile;

    @Before
    public void initGenerators() {
        generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        eventStreamCsvFile = TestUtils.getResource("/test_events.csv");
    }

    @Test
    public void shouldParseEntireFileWhenAllDecodersAreProvidedTest() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/updateStream_0.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST,
                WriteEventStreamReader.EVENT_DECODER_ADD_FORUM_MEMBERSHIP);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // When
        Iterator<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(Iterators.limit(new CsvFileReader(file, "\\|"), 4), eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldFailToParseEntireFileWhenOneDecoderNotProvidedAndAtLeastOnePolicyTest() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/updateStream_0.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // When
        Iterator<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(Iterators.limit(new CsvFileReader(file, "\\|"), 3), eventDescriptions);

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

    @Test
    public void shouldFailToParseEntireFileWhenOneDecoderNotProvidedAndExactlyOnePolicyTest() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/updateStream_0.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.EXACTLY_ONE_MATCH);

        // When
        Iterator<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(Iterators.limit(new CsvFileReader(file, "\\|"), 3), eventDescriptions);

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

    @Test
    public void shouldParseEntireFileWhenMultipleDecodersMatchSameEventAndAtLeastOncePolicy() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/updateStream_0.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST,
                WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST,
                WriteEventStreamReader.EVENT_DECODER_ADD_FORUM_MEMBERSHIP);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.AT_LEAST_ONE_MATCH);

        // When
        Iterator<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(Iterators.limit(new CsvFileReader(file, "\\|"), 4), eventDescriptions);

        // Then
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate2AddPostLike.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.next(), instanceOf(LdbcUpdate5AddForumMembership.class));
        assertThat(csvEventStreamReader.hasNext(), is(false));
    }

    @Test
    public void shouldFailToParseEntireFileWhenMultipleDecodersMatchSameEventAndExactlyOncePolicy() throws FileNotFoundException {
        // Given
        File file = TestUtils.getResource("/updateStream_0.csv");
        Iterable<EventDecoder<Operation<?>>> decoders = Lists.newArrayList(
                WriteEventStreamReader.EVENT_DECODER_ADD_LIKE_POST,
                WriteEventStreamReader.EVENT_DECODER_ADD_FORUM_MEMBERSHIP,
                WriteEventStreamReader.EVENT_DECODER_ADD_FORUM_MEMBERSHIP);
        EventDescriptions<Operation<?>> eventDescriptions = new EventDescriptions<>(decoders, EventReturnPolicy.EXACTLY_ONE_MATCH);

        // When
        Iterator<Operation<?>> csvEventStreamReader = new CsvEventStreamReader<>(Iterators.limit(new CsvFileReader(file, "\\|"), 4), eventDescriptions);

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
