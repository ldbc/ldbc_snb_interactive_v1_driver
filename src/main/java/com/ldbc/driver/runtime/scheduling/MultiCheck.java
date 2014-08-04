package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

import java.util.List;

/**
 * Passes if all checks pass.
 * doCheck() can be called multiple times.
 * Whenever one of the input checks passes it is removed from the list of checks that will be checked next doCheck().
 * Every time doCheck() is called only the checks that have no passed previously are executed.
 */
public class MultiCheck implements SpinnerCheck {
    private final List<SpinnerCheck> checks;
    private boolean allChecksHavePassed = false;

    public MultiCheck(List<SpinnerCheck> checks) {
        this.checks = checks;
    }

    @Override
    public boolean doCheck() {
        if (allChecksHavePassed) return true;
        boolean tempResult = true;
        for (int i = 0; i < checks.size(); i++) {
            SpinnerCheck check = checks.get(i);
            if (null == check) continue;
            if (check.doCheck()) {
                // check passed
                checks.set(i, null);
            } else {
                // check failed
                tempResult = false;
            }
        }
        allChecksHavePassed = tempResult;
        return allChecksHavePassed;
    }

    @Override
    public boolean handleFailedCheck(Operation<?> operation) {
        boolean result = true;
        for (SpinnerCheck check : checks)
            result = result && check.handleFailedCheck(operation);
        return result;
    }
}
