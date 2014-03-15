package com.ldbc.driver.runtime.scheduler;

import com.ldbc.driver.generator.Window;

// TODO test
public class IdentityScheduler<WINDOW_RETURN_TYPE, WINDOW_TYPE extends Window<?, WINDOW_RETURN_TYPE>>
        implements Scheduler<WINDOW_RETURN_TYPE, WINDOW_TYPE> {
    @Override
    public WINDOW_RETURN_TYPE schedule(WINDOW_TYPE handlersWindow) {
        return handlersWindow.contents();
    }
}
