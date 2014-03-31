package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

public interface SpinnerCheck {
    /**
     * Once a check has returned true it may never again return false
     *
     * @return
     */
    Boolean doCheck();

    void handleFailedCheck(Operation<?> operation);
}
