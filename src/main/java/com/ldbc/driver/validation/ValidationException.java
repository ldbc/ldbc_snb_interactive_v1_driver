package com.ldbc.driver.validation;

public class ValidationException extends Exception
{
    private static final long serialVersionUID = 8844396756042772132L;

    public ValidationException(String message)
    {
        super( message );
    }

    public ValidationException()
    {
        super();
    }

    public ValidationException(String message, Throwable cause)
    {
        super( message, cause );
    }

    public ValidationException(Throwable cause)
    {
        super( cause );
    }
}
