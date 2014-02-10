package com.ldbc.driver.metrics;

public class MetricException extends Exception {
    private static final long serialVersionUID = -5243628117143814942L;

    public MetricException(String message, Throwable cause) {
        super(message, cause);
    }

    public MetricException(String message) {
        super(message);
    }

    public MetricException(Throwable cause) {
        super(cause);
    }

    public MetricException() {
        super();
    }

}
