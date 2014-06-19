package com.ldbc.driver.temporal;

import java.util.concurrent.TimeUnit;

public interface TimeSource {
    Time now();

    // Avoid object creation where possible
    long nowAsMilli();
}