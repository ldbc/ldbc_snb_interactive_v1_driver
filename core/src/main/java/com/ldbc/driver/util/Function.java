package com.ldbc.driver.util;

public interface Function<F, T>
{
    T apply( F from );
}
