package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

import java.util.ArrayList;
import java.util.List;

/**
 * Passes if all checks pass.
 * doCheck() can be called multiple times.
 * Whenever one of the input checks passes it is removed from the list of checks that will be checked next doCheck().
 * Every time doCheck() is called only the checks that have no passed previously are executed.
 */
public class MultiCheck implements SpinnerCheck {
    private final List<SpinnerCheck> checks;

    public MultiCheck(List<SpinnerCheck> checks) {
        this.checks = checks;
    }

    @Override
    public Boolean doCheck() {
        if (checks.isEmpty()) return true;
        List<SpinnerCheck> checksToRemove = new ArrayList<>();
        for (int i = 0; i < checks.size(); i++) {
            SpinnerCheck check = checks.get(i);
            if (check.doCheck()) checksToRemove.add(check);
        }
        for (int i = 0; i < checksToRemove.size(); i++) {
            SpinnerCheck checkToRemove = checksToRemove.get(i);
            checks.remove(checkToRemove);
        }
        return checks.isEmpty();
    }

    @Override
    public void handleFailedCheck(Operation<?> operation) {
        for (SpinnerCheck check : checks)
            check.handleFailedCheck(operation);
    }
}
