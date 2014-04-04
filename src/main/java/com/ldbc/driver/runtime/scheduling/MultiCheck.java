package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

import java.util.ArrayList;
import java.util.List;

// TODO test
public class MultiCheck implements SpinnerCheck {
    private final List<SpinnerCheck> checks;

    public MultiCheck(List<SpinnerCheck> checks) {
        this.checks = checks;
    }

    @Override
    public Boolean doCheck() {
        if (checks.isEmpty()) return true;
        List<SpinnerCheck> checksToRemove = new ArrayList<SpinnerCheck>();
        for (SpinnerCheck check : checks)
            if (check.doCheck()) checksToRemove.add(check);
        for (SpinnerCheck checkToRemove : checksToRemove)
            checks.remove(checkToRemove);
        return checks.isEmpty();
    }

    @Override
    public void handleFailedCheck(Operation<?> operation) {
        for (SpinnerCheck check : checks)
            check.handleFailedCheck(operation);
    }
}
