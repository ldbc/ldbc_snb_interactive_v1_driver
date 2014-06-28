package com.ldbc.driver;

public class SerializingMarshallingException extends Exception {
    private static final long serialVersionUID = 6646883591588721475L;

    public SerializingMarshallingException(String message) {
        super(message);
    }

    public SerializingMarshallingException(String message, Throwable cause) {
        super(message, cause);
    }
}
