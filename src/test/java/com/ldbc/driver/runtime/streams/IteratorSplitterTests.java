package com.ldbc.driver.runtime.streams;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class IteratorSplitterTests {

    @Test
    public void shouldSplitIteratorCorrectlyGivenSimpleCase() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<Number>(IteratorSplitter.UnmappedItemPolicy.DROP);

        SplitDefinition<Number> byteAndIntegerDefinition = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> longDefinition = new SplitDefinition<Number>(Long.class);
        SplitDefinition<Number> doubleDefinition = new SplitDefinition<Number>(Double.class);

        // When
        SplitResult<Number> splitResult = iteratorSplitter.split(numbers.iterator(), byteAndIntegerDefinition, longDefinition, doubleDefinition);

        List<Number> integerSplit = Lists.newArrayList(splitResult.getSplitFor(byteAndIntegerDefinition));
        List<Number> longSplit = Lists.newArrayList(splitResult.getSplitFor(longDefinition));
        List<Number> doubleSplit = Lists.newArrayList(splitResult.getSplitFor(doubleDefinition));

        // Then
        assertThat(splitResult.count(), is(3));

        assertThat(integerSplit.size(), is(2));
        assertThat(integerSplit.get(0), instanceOf(Byte.class));
        assertThat(integerSplit.get(0).byteValue(), is((byte) 0));
        assertThat(integerSplit.get(1), instanceOf(Integer.class));
        assertThat(integerSplit.get(1).intValue(), is(1));

        assertThat(longSplit.size(), is(1));
        assertThat(longSplit.get(0), instanceOf(Long.class));
        assertThat(longSplit.get(0).longValue(), is(2l));

        assertThat(doubleSplit.size(), is(2));
        assertThat(doubleSplit.get(0), instanceOf(Double.class));
        assertThat(doubleSplit.get(0).doubleValue(), is(3d));
        assertThat(doubleSplit.get(1), instanceOf(Double.class));
        assertThat(doubleSplit.get(1).doubleValue(), is(4d));
    }

    @Test
    public void shouldDropUndefinedClassesFromSplit() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<Number>(IteratorSplitter.UnmappedItemPolicy.DROP);

        SplitDefinition<Number> byteAndIntegerDefinition = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> longDefinition = new SplitDefinition<Number>(Long.class);

        // When
        SplitResult<Number> splitResult = iteratorSplitter.split(numbers.iterator(), byteAndIntegerDefinition, longDefinition);

        List<Number> integerSplit = Lists.newArrayList(splitResult.getSplitFor(byteAndIntegerDefinition));
        List<Number> longSplit = Lists.newArrayList(splitResult.getSplitFor(longDefinition));

        // Then
        assertThat(splitResult.count(), is(2));

        assertThat(integerSplit.size(), is(2));
        assertThat(integerSplit.get(0), instanceOf(Byte.class));
        assertThat(integerSplit.get(0).byteValue(), is((byte) 0));
        assertThat(integerSplit.get(1), instanceOf(Integer.class));
        assertThat(integerSplit.get(1).intValue(), is(1));

        assertThat(longSplit.size(), is(1));
        assertThat(longSplit.get(0), instanceOf(Long.class));
        assertThat(longSplit.get(0).longValue(), is(2l));
    }

    @Test
    public void shouldDropAllWhenNoDefinitionsGiven() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<Number>(IteratorSplitter.UnmappedItemPolicy.DROP);

        // When
        SplitResult<Number> splitResult = iteratorSplitter.split(numbers.iterator());

        // Then
        assertThat(splitResult.count(), is(0));
    }

    @Test
    public void shouldThrowExceptionOnUndefinedClassesFromSplit() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<Number>(IteratorSplitter.UnmappedItemPolicy.ABORT);

        SplitDefinition<Number> byteAndIntegerDefinition = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> longDefinition = new SplitDefinition<Number>(Long.class);

        // When
        boolean exceptionThrown = false;
        try {
            iteratorSplitter.split(numbers.iterator(), byteAndIntegerDefinition, longDefinition);
        } catch (IteratorSplittingException e) {
            exceptionThrown = true;
        }

        // Then
        assertThat(exceptionThrown, is(true));
    }
}
