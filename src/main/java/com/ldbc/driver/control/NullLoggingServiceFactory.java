package com.ldbc.driver.control;

public class NullLoggingServiceFactory implements LoggingServiceFactory
{
    @Override
    public LoggingService loggingServiceFor( String source )
    {
        return new NullLoggingService();
    }
}
