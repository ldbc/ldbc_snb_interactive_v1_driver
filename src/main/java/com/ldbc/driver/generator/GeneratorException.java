package com.ldbc.driver.generator;

public class GeneratorException extends RuntimeException {
    private static final long serialVersionUID = -5243628117143814942L;

    public GeneratorException(String message, Throwable cause) {
        super(message, cause);
    }

    public GeneratorException(String message) {
        super(message);
    }
}
