package com.ldbc.driver.runtime.streams;

public class IteratorSplittingException extends Exception {
    private static final long serialVersionUID = -5243628117143814942L;

    public IteratorSplittingException(String message) {
        super(message);
    }

    public IteratorSplittingException(String message, Throwable cause) {
        super(message, cause);
    }
}
