package org.ldbcouncil.driver.control;

public interface LoggingServiceFactory
{
    LoggingService loggingServiceFor( String source );
}
