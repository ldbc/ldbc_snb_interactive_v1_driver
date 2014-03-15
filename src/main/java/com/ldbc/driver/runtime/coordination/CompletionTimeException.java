package com.ldbc.driver.runtime.coordination;

public class CompletionTimeException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public CompletionTimeException(String message)
    {
        super( message );
    }

    public CompletionTimeException(String message, Throwable cause)
    {
        super( message, cause );
    }
}
