package com.ldbc.driver.control;

public interface LoggingServiceFactory
{
    LoggingService loggingServiceFor( String source );
}
