package com.ldbc.driver.util;

public interface Function1<INPUT, RETURN> {
    RETURN apply(INPUT input);
}
