package com.ldbc.driver.runtime.scheduling;

import com.google.common.collect.Lists;
import com.ldbc.driver.Operation;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class MultiCheckTest {
    static final SpinnerCheck TRUE_CHECK = new SpinnerCheck() {
        @Override
        public boolean doCheck() {
            return true;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            return true;
        }
    };

    static final SpinnerCheck FALSE_CHECK = new SpinnerCheck() {
        @Override
        public boolean doCheck() {
            return false;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            return false;
        }
    };

    class SettableSpinnerCheck implements SpinnerCheck {
        private boolean checkResult = false;

        SettableSpinnerCheck(boolean checkResult) {
            this.checkResult = checkResult;
        }

        void setCheckResult(boolean checkResult) {
            this.checkResult = checkResult;
        }

        @Override
        public boolean doCheck() {
            return checkResult;
        }

        @Override
        public boolean handleFailedCheck(Operation<?> operation) {
            return false;
        }
    }

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

    @Test
    public void shouldHaveMonotonicResultOnceItHasBeenTrueItCanNeverBeFalseAgain() {
        // Given
        SettableSpinnerCheck check = new SettableSpinnerCheck(false);
        MultiCheck multiCheck = new MultiCheck(Lists.<SpinnerCheck>newArrayList(check));

        // When/When
        check.setCheckResult(false);
        assertThat(multiCheck.doCheck(), is(false));

        check.setCheckResult(true);
        assertThat(multiCheck.doCheck(), is(true));

        check.setCheckResult(false);
        assertThat(multiCheck.doCheck(), is(true));
    }

    @Test
    public void shouldHaveMonotonicResultOnceItHasBeenTrueItCanNeverBeFalseAgainWithManyChecks() {
        // Given
        SettableSpinnerCheck check1 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check2 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check3 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check4 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check5 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check6 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check7 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check8 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check9 = new SettableSpinnerCheck(false);
        SettableSpinnerCheck check10 = new SettableSpinnerCheck(false);
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
        check1.setCheckResult(false);
        check2.setCheckResult(false);
        check3.setCheckResult(false);
        check4.setCheckResult(false);
        check5.setCheckResult(false);
        check6.setCheckResult(false);
        check7.setCheckResult(false);
        check8.setCheckResult(false);
        check9.setCheckResult(false);
        check10.setCheckResult(false);
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(false);
        check3.setCheckResult(false);
        check4.setCheckResult(false);
        check5.setCheckResult(false);
        check6.setCheckResult(false);
        check7.setCheckResult(false);
        check8.setCheckResult(false);
        check9.setCheckResult(false);
        check10.setCheckResult(false);
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(false);
        check3.setCheckResult(true); // true
        check4.setCheckResult(false);
        check5.setCheckResult(false);
        check6.setCheckResult(false);
        check7.setCheckResult(false);
        check8.setCheckResult(false);
        check9.setCheckResult(false);
        check10.setCheckResult(false);
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(false);
        check3.setCheckResult(true); // true
        check4.setCheckResult(false);
        check5.setCheckResult(true); // true
        check6.setCheckResult(false);
        check7.setCheckResult(false);
        check8.setCheckResult(false);
        check9.setCheckResult(false);
        check10.setCheckResult(false);
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(false);
        check3.setCheckResult(true); // true
        check4.setCheckResult(false);
        check5.setCheckResult(true); // true
        check6.setCheckResult(false);
        check7.setCheckResult(true); // true
        check8.setCheckResult(false);
        check9.setCheckResult(false);
        check10.setCheckResult(true); // true
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(false);
        check3.setCheckResult(true); // true
        check4.setCheckResult(false);
        check5.setCheckResult(true); // true
        check6.setCheckResult(true); // true
        check7.setCheckResult(true); // true
        check8.setCheckResult(false);
        check9.setCheckResult(false);
        check10.setCheckResult(true); // true
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(false);
        check3.setCheckResult(true); // true
        check4.setCheckResult(false);
        check5.setCheckResult(true); // true
        check6.setCheckResult(true); // true
        check7.setCheckResult(true); // true
        check8.setCheckResult(true); // true
        check9.setCheckResult(false);
        check10.setCheckResult(true); // true
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(true); // true
        check2.setCheckResult(true); // true
        check3.setCheckResult(true); // true
        check4.setCheckResult(false);
        check5.setCheckResult(true); // true
        check6.setCheckResult(true); // true
        check7.setCheckResult(true); // true
        check8.setCheckResult(true); // true
        check9.setCheckResult(true); // true
        check10.setCheckResult(true); // true
        assertThat(multiCheck.doCheck(), is(false));

        check1.setCheckResult(false); // true
        check2.setCheckResult(false); // true
        check3.setCheckResult(false); // true
        check4.setCheckResult(true); // true
        check5.setCheckResult(false); // true
        check6.setCheckResult(false); // true
        check7.setCheckResult(false); // true
        check8.setCheckResult(false); // true
        check9.setCheckResult(false); // true
        check10.setCheckResult(false); // true
        assertThat(multiCheck.doCheck(), is(true));
    }
}
