package com.ldbc.driver.runtime.executor;

public class OperationExecutorException extends Exception {
    private static final long serialVersionUID = -5243628117143814942L;

    public OperationExecutorException(String message) {
        super(message);
    }

    public OperationExecutorException(String message, Throwable cause) {
        super(message, cause);
    }
}
