package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

import java.util.List;

/**
 * Passes if all checks pass.
 * doCheck() can be called multiple times.
 * Whenever one of the input checks passes it is removed from the list of checks that will be checked next doCheck()
 * Every time doCheck() is called only the checks that have not passed previously are executed
 * This check is monotonic in the sense that:
 * - STILL_CHECKING --> PASSED    OK
 * - STILL_CHECKING --> FAILED    OK
 * - FAILED --> _                 NOT OK
 * - PASSED --> _                 NOT OK
 */
public class MultiCheck implements SpinnerCheck {
    private final List<SpinnerCheck> checks;
    private SpinnerCheckResult checkResult = SpinnerCheckResult.STILL_CHECKING;

    public MultiCheck(List<SpinnerCheck> checks) {
        this.checks = checks;
    }

    @Override
    public SpinnerCheckResult doCheck() {
        if (SpinnerCheckResult.STILL_CHECKING != checkResult) {
            return checkResult;
        }
        SpinnerCheckResult tempCheckResult = SpinnerCheckResult.PASSED;
        for (int i = 0; i < checks.size(); i++) {
            SpinnerCheck check = checks.get(i);
            if (null == check) continue;
            switch (check.doCheck()) {
                case PASSED:
                    // remove check
                    checks.set(i, null);
                    break;
                case STILL_CHECKING:
                    tempCheckResult = SpinnerCheckResult.STILL_CHECKING;
                    break;
                case FAILED:
                    checkResult = SpinnerCheckResult.FAILED;
                    return checkResult;
            }
        }
        checkResult = tempCheckResult;
        return checkResult;
    }

    @Override
    public boolean handleFailedCheck(Operation<?> operation) {
        boolean result = true;
        for (SpinnerCheck check : checks)
            result = result && check.handleFailedCheck(operation);
        return result;
    }
}
