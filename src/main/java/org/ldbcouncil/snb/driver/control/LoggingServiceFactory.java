package org.ldbcouncil.snb.driver.control;

public interface LoggingServiceFactory
{
    LoggingService loggingServiceFor( String source );
}
