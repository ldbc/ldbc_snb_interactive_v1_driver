package com.ldbc.driver.runtime.scheduler;

import com.ldbc.driver.generator.Window;

public interface Scheduler<WINDOW_RETURN_TYPE, WINDOW_TYPE extends Window<?, WINDOW_RETURN_TYPE>> {
    WINDOW_RETURN_TYPE schedule(WINDOW_TYPE handlersWindow);
}
