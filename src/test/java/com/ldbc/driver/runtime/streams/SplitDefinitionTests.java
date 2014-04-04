package com.ldbc.driver.runtime.streams;

import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SplitDefinitionTests {

    @Test
    public void shouldAllowCreationOfNonEmptyDefinition() throws IteratorSplittingException {
        // Given
        Class<? extends Number> numberClasses[] = new Class[]{Byte.class, Integer.class};
        boolean exceptionThrown = false;
        SplitDefinition<Number> numbersDefinition = null;

        // When
        try {
            numbersDefinition = new SplitDefinition<Number>(numberClasses);
        } catch (IteratorSplittingException e) {
            exceptionThrown = true;
        }
        List<Class<? extends Number>> definedNumberClasses = Lists.newArrayList(numbersDefinition.itemTypes());

        // Then
        assertThat(exceptionThrown, is(false));
        assertThat(definedNumberClasses.size(), is(2));
        assertThat(definedNumberClasses.contains(Byte.class), is(true));
        assertThat(definedNumberClasses.contains(Integer.class), is(true));
    }

    @Test
    public void shouldNotAllowCreationOfNullParameterDefinition() throws IteratorSplittingException {
        // Given
        boolean exceptionThrown = false;

        // When
        try {
            new SplitDefinition<Number>(null);
        } catch (IteratorSplittingException e) {
            exceptionThrown = true;
        }

        // Then
        assertThat(exceptionThrown, is(true));
    }

    @Test
    public void twoDefinitionsWithSameParametersInAnyOrderShouldBeEqual() throws IteratorSplittingException {
        // Given
        SplitDefinition<Number> definition1 = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> definition2 = new SplitDefinition<Number>(Integer.class, Byte.class);

        // When
        boolean definitionsAreEqual = definition1.equals(definition2);

        // Then
        assertThat(definitionsAreEqual, is(true));
    }

    @Test
    public void twoDefinitionsWithOneOrMoreParametersInCommonShouldOverlap() throws IteratorSplittingException {
        // Given
        SplitDefinition<Number> definition1 = new SplitDefinition<Number>(Byte.class, Double.class);
        SplitDefinition<Number> definition2 = new SplitDefinition<Number>(Byte.class, Integer.class);
        SplitDefinition<Number> definition3 = new SplitDefinition<Number>(Integer.class);

        // When
        boolean definitions1and1Overlap = definition1.overlapsWith(definition1);
        boolean definitions1and2Overlap = definition1.overlapsWith(definition2);
        boolean definitions1and3Overlap = definition1.overlapsWith(definition3);
        boolean definitions2and3Overlap = definition2.overlapsWith(definition3);

        // Then
        assertThat(definitions1and1Overlap, is(true));
        assertThat(definitions1and2Overlap, is(true));
        assertThat(definitions1and3Overlap, is(false));
        assertThat(definitions2and3Overlap, is(true));
    }
}
