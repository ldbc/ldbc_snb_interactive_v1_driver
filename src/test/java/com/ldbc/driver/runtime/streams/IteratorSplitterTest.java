package com.ldbc.driver.runtime.streams;

import com.google.common.collect.Lists;
import com.ldbc.driver.generator.GeneratorFactory;
import com.ldbc.driver.runtime.ConcurrentErrorReporter;
import com.ldbc.driver.util.RandomDataGeneratorFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;

@Ignore
public class IteratorSplitterTest {

    @Test
    public void shouldSplitIteratorCorrectlyGivenSimpleCaseAndSmallInput() throws IteratorSplittingException {
        for (int i = 0; i < 20; i++) {
            doShouldSplitIteratorCorrectlyGivenSimpleCaseAndSmallInput();
        }
    }

    public void doShouldSplitIteratorCorrectlyGivenSimpleCaseAndSmallInput() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.DROP);

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

    @Ignore
    @Test
    public void shouldSplitIteratorCorrectlyGivenSimpleCaseAndLargeInput() throws IteratorSplittingException {
        for (int i = 0; i < 20; i++) {
            doShouldSplitIteratorCorrectlyGivenSimpleCaseAndLargeInput();
        }
    }

    public void doShouldSplitIteratorCorrectlyGivenSimpleCaseAndLargeInput() throws IteratorSplittingException {
        // Given
        GeneratorFactory generators = new GeneratorFactory(new RandomDataGeneratorFactory(42L));
        Iterable<? extends Number> possibleNumbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));
        Iterator<? extends Number> numbersGenerator = generators.discrete(possibleNumbers);
        int numberCount = 1000000;
        Iterator<? extends Number> numbers = generators.limit(numbersGenerator, numberCount);

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.DROP);

        SplitDefinition<Number> byteAndIntegerDefinition = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> longDefinition = new SplitDefinition<Number>(Long.class);
        SplitDefinition<Number> doubleDefinition = new SplitDefinition<Number>(Double.class);

        // When
        SplitResult<Number> splitResult = iteratorSplitter.split(numbers, byteAndIntegerDefinition, longDefinition, doubleDefinition);

        List<Number> integerSplit = Lists.newArrayList(splitResult.getSplitFor(byteAndIntegerDefinition));
        List<Number> longSplit = Lists.newArrayList(splitResult.getSplitFor(longDefinition));
        List<Number> doubleSplit = Lists.newArrayList(splitResult.getSplitFor(doubleDefinition));

        // Then
        assertThat(splitResult.count(), is(3));

        assertThat(integerSplit.size() + longSplit.size() + doubleSplit.size(), is(numberCount));
        assertThat(integerSplit.get(0), anyOf(instanceOf(Byte.class), instanceOf(Integer.class)));
        assertThat(longSplit.get(0), instanceOf(Long.class));
        assertThat(doubleSplit.get(0), instanceOf(Double.class));
    }

    @Test
    public void shouldDropUndefinedClassesFromSplit() throws IteratorSplittingException {
        for (int i = 0; i < 20; i++) {
            doShouldDropUndefinedClassesFromSplit();
        }
    }

    public void doShouldDropUndefinedClassesFromSplit() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.DROP);

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
        for (int i = 0; i < 20; i++) {
            doShouldDropAllWhenNoDefinitionsGiven();
        }
    }

    public void doShouldDropAllWhenNoDefinitionsGiven() throws IteratorSplittingException {
        // Given
        List<? extends Number> numbers = Lists.newArrayList(new Byte((byte) 0), new Integer(1), new Long(2), new Double(3), new Double(4));

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.DROP);

        // When
        SplitResult<Number> splitResult = iteratorSplitter.split(numbers.iterator());

        // Then
        assertThat(splitResult.count(), is(0));
    }

    @Test
    public void shouldThrowExceptionOnUndefinedClassesFromSplitWithLongStream() throws IteratorSplittingException {
        for (int i = 0; i < 20; i++) {
            doShouldThrowExceptionOnUndefinedClassesFromSplitWithLongStream();
        }
    }

    public void doShouldThrowExceptionOnUndefinedClassesFromSplitWithLongStream() throws IteratorSplittingException {
        // Given
        GeneratorFactory gf = new GeneratorFactory(new RandomDataGeneratorFactory(42l));

        List<Number> numbers = Lists.newArrayList(gf.limit(gf.repeating(gf.<Number>identity((byte) 1, 2, 3l)), 10000));
        numbers.add(4d);

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);

        SplitDefinition<Number> byteAndIntegerDefinition = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> longDefinition = new SplitDefinition<Number>(Long.class);

        // When
        boolean exceptionThrown = false;
        String errMsg = "";
        try {
            iteratorSplitter.split(numbers.iterator(), byteAndIntegerDefinition, longDefinition);
        } catch (IteratorSplittingException e) {
            errMsg = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }

        // Then
        assertThat(errMsg, exceptionThrown, is(true));
    }

    @Test
    public void shouldThrowExceptionOnUndefinedClassesFromSplitWithVeryShortStream() throws IteratorSplittingException {
        for (int i = 0; i < 20; i++) {
            doShouldThrowExceptionOnUndefinedClassesFromSplitWithVeryShortStream();
        }
    }

    public void doShouldThrowExceptionOnUndefinedClassesFromSplitWithVeryShortStream() throws IteratorSplittingException {
        // Given
        List<Number> numbers = Lists.<Number>newArrayList(1d);

        IteratorSplitter<Number> iteratorSplitter = new IteratorSplitter<>(IteratorSplitter.UnmappedItemPolicy.ABORT);

        SplitDefinition<Number> longDefinition = new SplitDefinition<Number>(Long.class);

        // When
        boolean exceptionThrown = false;
        String errMsg = "";
        try {
            iteratorSplitter.split(numbers.iterator(), longDefinition);
        } catch (IteratorSplittingException e) {
            errMsg = ConcurrentErrorReporter.stackTraceToString(e);
            exceptionThrown = true;
        }

        // Then
        assertThat(errMsg, exceptionThrown, is(true));
    }
}
