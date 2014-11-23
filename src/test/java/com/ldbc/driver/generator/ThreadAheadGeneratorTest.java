package com.ldbc.driver.generator;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ThreadAheadGeneratorTest {
    @Test
    public void shouldReturnSameContentsAsInputIterator() {
        // Given
        int threadAheadDistance = 1000;
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        List<Integer> emptyBefore = Lists.newArrayList();
        List<Integer> smallBefore = Lists.newArrayList(1, 2, 3, 4, 5);
        List<Integer> largeBefore = Lists.newArrayList(gf.limit(gf.incrementing(1, 1), 1000000));

        // When
        List<Integer> emptyAfter = Lists.newArrayList(gf.threadAhead(emptyBefore.iterator(), threadAheadDistance));
        List<Integer> smallAfter = Lists.newArrayList(gf.threadAhead(smallBefore.iterator(), threadAheadDistance));
        List<Integer> largeAfter = Lists.newArrayList(gf.threadAhead(largeBefore.iterator(), threadAheadDistance));

        // Then
        assertThat(emptyAfter, equalTo(emptyBefore));
        assertThat(smallAfter, equalTo(smallBefore));
        assertThat(largeAfter, equalTo(largeBefore));
    }

    @Test
    public void shouldWouldWithVeryLargeIterators() {
        // Given
        long first = 1;
        long count = 10000000;
        int threadAheadDistance = 1000;
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        // When
        Iterator<Long> before = gf.limit(gf.incrementing(first, 1l), count);
        Iterator<Long> after = gf.threadAhead(gf.limit(gf.incrementing(first, 1l), count), threadAheadDistance);

        // Then
        assertThat(Iterators.elementsEqual(before, after), is(true));
    }

    @Test
    public void shouldReturnNoMoreItemsAfterShutdown() {
        // Given
        int threadAheadDistance = 1000;
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));
        Iterator<Long> before = gf.limit(gf.incrementing(1l, 1l), 1000);

        // When
        ThreadAheadGenerator<Long> after = gf.threadAhead(before, threadAheadDistance);

        assertThat(after.hasNext(), equalTo(true));
        assertThat(after.next(), equalTo(1l));
        assertThat(after.hasNext(), equalTo(true));
        assertThat(after.next(), equalTo(2l));
        assertThat(after.hasNext(), equalTo(true));
        assertThat(after.next(), equalTo(3l));

        after.forceShutdown();

        // Then
        assertThat(after.hasNext(), equalTo(false));
    }
}
