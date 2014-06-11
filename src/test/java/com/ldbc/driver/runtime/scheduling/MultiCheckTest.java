package com.ldbc.driver.runtime.scheduling;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiCheckTest {
    static final SpinnerCheck TRUE_CHECK = new SpinnerCheck() {
        @Override
        public Boolean doCheck() {
            return true;
        }

        @Override
        public void handleFailedCheck(Operation<?> operation) {
        }
    };

    static final SpinnerCheck FALSE_CHECK = new SpinnerCheck() {
        @Override
        public Boolean doCheck() {
            return false;
        }

        @Override
        public void handleFailedCheck(Operation<?> operation) {
        }
    };

    @Test
    public void shouldPassWhenThereAreNoChecksToPerform() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList());

        // When
        boolean allChecksPassed = multiCheck.doCheck();

        // Then
        assertThat(allChecksPassed, is(true));
    }

    @Test
    public void shouldPassWithOneTrueCheck() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(TRUE_CHECK));

        // When
        boolean allChecksPassed = multiCheck.doCheck();

        // Then
        assertThat(allChecksPassed, is(true));
    }

    @Test
    public void shouldPassWithMultipleTrueChecks() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(TRUE_CHECK, TRUE_CHECK, TRUE_CHECK, TRUE_CHECK));

        // When
        boolean allChecksPassed = multiCheck.doCheck();

        // Then
        assertThat(allChecksPassed, is(true));
    }

    @Test
    public void shouldFailWithOneFalseCheck() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(FALSE_CHECK));

        // When
        boolean allChecksPassed = multiCheck.doCheck();

        // Then
        assertThat(allChecksPassed, is(false));
    }

    @Test
    public void shouldFailWithMultipleFalseChecks() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(FALSE_CHECK, FALSE_CHECK, FALSE_CHECK, FALSE_CHECK));

        // When
        boolean allChecksPassed = multiCheck.doCheck();

        // Then
        assertThat(allChecksPassed, is(false));
    }

    @Test
    public void shouldFailWhenAtLeastOneCheckIsFalse() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(TRUE_CHECK, TRUE_CHECK, TRUE_CHECK, TRUE_CHECK, FALSE_CHECK, TRUE_CHECK));

        // When
        boolean allChecksPassed = multiCheck.doCheck();

        // Then
        assertThat(allChecksPassed, is(false));
    }
}
