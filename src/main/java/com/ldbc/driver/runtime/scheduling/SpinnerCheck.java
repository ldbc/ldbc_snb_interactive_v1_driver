package com.ldbc.driver.runtime.scheduling;

import com.ldbc.driver.Operation;

public interface SpinnerCheck {
    Boolean doCheck();

    void handleFailedCheck(Operation<?> operation);
}
