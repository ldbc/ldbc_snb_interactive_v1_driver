package com.ldbc.driver.runtime.metrics;

public class MetricsCollectionException extends Exception
{
    private static final long serialVersionUID = 6646883591588721475L;

    public MetricsCollectionException(String message)
    {
        super( message );
    }

    public MetricsCollectionException(String message, Throwable cause)
    {
        super( message, cause );
    }
}
