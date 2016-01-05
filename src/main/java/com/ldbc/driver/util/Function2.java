package com.ldbc.driver.util;

public interface Function2<INPUT1, INPUT2, RETURN, EXCEPTION extends Exception>
{
    RETURN apply( INPUT1 input1, INPUT2 input2 ) throws EXCEPTION;
}
