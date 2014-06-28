package com.ldbc.driver;

public class ClientException extends Exception {
    private static final long serialVersionUID = 7166804842129940500L;

    public ClientException(String message) {
        super(message);
    }

    public ClientException(String message, Throwable cause) {
        super(message, cause);
    }
}
