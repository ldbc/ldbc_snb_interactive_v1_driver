package com.ldbc.driver;

public class WorkloadException extends Exception {
    private static final long serialVersionUID = 8844396756042772132L;

    public WorkloadException(String message) {
        super(message);
    }

    public WorkloadException(String message, Throwable cause) {
        super(message, cause);
    }
}
