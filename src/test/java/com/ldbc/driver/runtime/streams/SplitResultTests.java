package com.ldbc.driver.runtime.streams;

import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SplitResultTests {

    @Test
    public void shouldReturnAllCreatedSplits() throws IteratorSplittingException {
        // Given
        SplitResult<Number> splitResult = new SplitResult<Number>();
        SplitDefinition<Number> definition1 = new SplitDefinition<Number>(Byte.class, Double.class);
        SplitDefinition<Number> definition2 = new SplitDefinition<Number>(Integer.class);
        SplitDefinition<Number> definition3 = new SplitDefinition<Number>(Long.class);

        // When
        List<Number> split1 = splitResult.addSplit(definition1);
        List<Number> split2 = splitResult.addSplit(definition2);
        List<Number> split3 = splitResult.addSplit(definition3);

        // Then
        assertThat(splitResult.count(), is(3));
        assertThat(splitResult.getSplitFor(definition1), equalTo((Iterable) split1));
        assertThat(splitResult.getSplitFor(definition2), equalTo((Iterable) split2));
        assertThat(splitResult.getSplitFor(definition3), equalTo((Iterable) split3));
    }

    @Test
    public void shouldThrowExceptionIfOverlappingDefinitionsAreAdded() throws IteratorSplittingException {
        // Given
        SplitResult<Number> splitResult = new SplitResult<Number>();
        SplitDefinition<Number> definition1 = new SplitDefinition<Number>(Byte.class, Double.class);
        SplitDefinition<Number> definition2 = new SplitDefinition<Number>(Integer.class);
        SplitDefinition<Number> definition3 = new SplitDefinition<Number>(Byte.class);
        boolean exceptionThrownWhenAddingOverlappingDefinition = false;
        boolean exceptionThrownWhenRetrievingNonExistentDefinition = false;

        // When
        List<Number> split1 = splitResult.addSplit(definition1);
        List<Number> split2 = splitResult.addSplit(definition2);
        try {
            splitResult.addSplit(definition3);
        } catch (IteratorSplittingException e) {
            exceptionThrownWhenAddingOverlappingDefinition = true;
        }
        try {
            splitResult.getSplitFor(definition3);
        } catch (IteratorSplittingException e) {
            exceptionThrownWhenRetrievingNonExistentDefinition = true;
        }

        // Then
        assertThat(exceptionThrownWhenAddingOverlappingDefinition, is(true));
        assertThat(exceptionThrownWhenRetrievingNonExistentDefinition, is(true));
        assertThat(splitResult.count(), is(2));
        assertThat(splitResult.getSplitFor(definition1), equalTo((Iterable) split1));
        assertThat(splitResult.getSplitFor(definition2), equalTo((Iterable) split2));
    }
}
