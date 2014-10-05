package com.ldbc.driver.runtime.scheduling;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiCheckTest {
    static final SpinnerCheck TRUE_CHECK = new SpinnerCheck() {
        @Override
        public SpinnerCheckResult doCheck() {
            return SpinnerCheckResult.PASSED;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            return true;
        }
    };

    static final SpinnerCheck FALSE_CHECK = new SpinnerCheck() {
        @Override
        public SpinnerCheckResult doCheck() {
            return SpinnerCheckResult.FAILED;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            return false;
        }
    };

    @Test
    public void shouldPassWhenThereAreNoChecksToPerform() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList());

        // When
        SpinnerCheck.SpinnerCheckResult resultOfAllChecks = multiCheck.doCheck();

        // Then
        assertThat(resultOfAllChecks, is(SpinnerCheck.SpinnerCheckResult.PASSED));
    }

    @Test
    public void shouldPassWithOneTrueCheck() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(TRUE_CHECK));

        // When
        SpinnerCheck.SpinnerCheckResult resultOfAllChecks = multiCheck.doCheck();

        // Then
        assertThat(resultOfAllChecks, is(SpinnerCheck.SpinnerCheckResult.PASSED));
    }

    @Test
    public void shouldPassWithMultipleTrueChecks() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(TRUE_CHECK, TRUE_CHECK, TRUE_CHECK, TRUE_CHECK));

        // When
        SpinnerCheck.SpinnerCheckResult resultOfAllChecks = multiCheck.doCheck();

        // Then
        assertThat(resultOfAllChecks, is(SpinnerCheck.SpinnerCheckResult.PASSED));
    }

    @Test
    public void shouldFailWithOneFalseCheck() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(FALSE_CHECK));

        // When
        SpinnerCheck.SpinnerCheckResult resultOfAllChecks = multiCheck.doCheck();

        // Then
        assertThat(resultOfAllChecks, is(SpinnerCheck.SpinnerCheckResult.FAILED));
    }

    @Test
    public void shouldFailWithMultipleFalseChecks() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(FALSE_CHECK, FALSE_CHECK, FALSE_CHECK, FALSE_CHECK));

        // When
        SpinnerCheck.SpinnerCheckResult resultOfAllChecks = multiCheck.doCheck();

        // Then
        assertThat(resultOfAllChecks, is(SpinnerCheck.SpinnerCheckResult.FAILED));
    }

    @Test
    public void shouldFailWhenAtLeastOneCheckIsFalse() {
        // Given
        MultiCheck multiCheck = new MultiCheck(Lists.newArrayList(TRUE_CHECK, TRUE_CHECK, TRUE_CHECK, TRUE_CHECK, FALSE_CHECK, TRUE_CHECK));

        // When
        SpinnerCheck.SpinnerCheckResult resultOfAllChecks = multiCheck.doCheck();

        // Then
        assertThat(resultOfAllChecks, is(SpinnerCheck.SpinnerCheckResult.FAILED));
    }

    // STILL_CHECKING --> PASSED    OK
    // STILL_CHECKING --> FAILED    OK
    // FAILED --> _                 NOT OK
    // PASSED --> _                 NOT OK
    @Test
    public void shouldHaveMonotonicResult() {
        SettableSpinnerCheck check;
        MultiCheck multiCheck;

        // Given
        check = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList(check));

        // When/Then
        check.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.FAILED));
        check.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.FAILED));
        check.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.FAILED));

        // Given
        check = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList(check));

        // When/Then
        check.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));
        check.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));
        check.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));
    }

    @Test
    public void shouldNotPassUntilAllHaveChecksHavePassedAndThenShouldNotBeAbleToGoBackToFailedOrStillChecking() {
        // Given
        SettableSpinnerCheck check1 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check2 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check3 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check4 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check5 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check6 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check7 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check8 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check9 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check10 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        MultiCheck multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList(
                check1,
                check2,
                check3,
                check4,
                check5,
                check6,
                check7,
                check8,
                check9,
                check10));

        // When/When
        check1.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED); // true
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.PASSED));
    }

    @Test
    public void ifAnyChecksFailEntireCheckShouldFailAndThenShouldNotBeAbleToGoBackToFailedOrStillChecking() {
        // Given
        SettableSpinnerCheck check1 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check2 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check3 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check4 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check5 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check6 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check7 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check8 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check9 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        SettableSpinnerCheck check10 = new SettableSpinnerCheck(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        MultiCheck multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList(
                check1,
                check2,
                check3,
                check4,
                check5,
                check6,
                check7,
                check8,
                check9,
                check10));

        // When/When
        check1.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED); // true
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.PASSED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.FAILED));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.FAILED);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.FAILED));

        check1.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check2.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check3.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check4.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check5.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check6.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check7.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check8.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check9.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        check10.setResult(SpinnerCheck.SpinnerCheckResult.STILL_CHECKING);
        assertThat(multiCheck.doCheck(), is(SpinnerCheck.SpinnerCheckResult.FAILED));    }
}
