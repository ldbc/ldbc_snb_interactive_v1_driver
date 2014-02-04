package com.ldbc.driver.coordination;

public class CompletionTimeException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public CompletionTimeException(String message)
    {
        super( message );
    }

    public CompletionTimeException()
    {
        super();
    }

    public CompletionTimeException(String message, Throwable cause)
    {
        super( message, cause );
    }

    public CompletionTimeException(Throwable cause)
    {
        super( cause );
    }
}
